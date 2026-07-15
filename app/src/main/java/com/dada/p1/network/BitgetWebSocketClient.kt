package com.dada.p1.network

import com.dada.p1.network.dto.BitgetArg
import com.dada.p1.network.dto.BitgetEnvelope
import com.dada.p1.network.dto.BitgetSubRequest
import com.dada.p1.network.dto.KlineEntry
import com.dada.p1.network.dto.OrderBookSnapshot
import com.dada.p1.network.dto.OrderBookWire
import com.dada.p1.network.dto.TickerData
import com.dada.p1.network.dto.TickerWire
import com.dada.p1.network.dto.toKlineEntries
import com.dada.p1.network.dto.toSnapshot
import com.dada.p1.network.dto.toTickerData
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

class BitgetWebSocketClient(
    private val instId: String = "BTCUSDT",
    private val instType: String = "SPOT",
    private val channels: List<BitgetChannel> = listOf(
        BitgetChannel.OrderBook(15),
        BitgetChannel.Ticker,
        BitgetChannel.Kline("1m")
    ),
    private val wsUrl: String = "wss://ws.bitget.com/v2/ws/public",
    private val pingIntervalMs: Long = 25_000L, // must stay below Bitget's 30s idle-disconnect window
    private val pongTimeoutMs: Long = 10_000L,  // grace period after a ping before we treat the link as dead
    private val baseBackoffMs: Long = 1_000L,
    private val maxBackoffMs: Long = 30_000L,
    private val maxBackoffAttempts: Int = 10 // backoff stops growing after this many attempts, keeps retrying at cap
) {
    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor + Dispatchers.Default)

    private val okHttpClient = OkHttpClient.Builder()
        .pingInterval(0, TimeUnit.SECONDS) // disabled: Bitget uses app-level text ping/pong, not WS control frames
        .readTimeout(0, TimeUnit.MILLISECONDS) // socket is long-lived; timeout handled by our own heartbeat watchdog
        .build()

    private val json = Json { ignoreUnknownKeys = true }

    private var webSocket: WebSocket? = null
    private var heartbeatJob: Job? = null
    private var reconnectJob: Job? = null
    private val reconnectAttempts = AtomicInteger(0)
    private val lastPongAt = AtomicLong(0L)
    private val manuallyClosed = AtomicInteger(0) // 0 = false, 1 = true; avoids reconnect after explicit disconnect()

    private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Idle)
    val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

    // Latest full book per side, keyed by instId, kept as StateFlow since consumers usually want "current" state
    private val _orderBookFlow = MutableStateFlow<OrderBookSnapshot?>(null)
    val orderBookFlow: StateFlow<OrderBookSnapshot?> = _orderBookFlow.asStateFlow()

    private val _tickerFlow = MutableStateFlow<TickerData?>(null)
    val tickerFlow: StateFlow<TickerData?> = _tickerFlow.asStateFlow()

    // Kline prints are discrete events, not "current state", so a SharedFlow of buffered replay is more appropriate
    private val _klineFlow = MutableSharedFlow<List<KlineEntry>>(replay = 0, extraBufferCapacity = 64)
    val klineFlow: SharedFlow<List<KlineEntry>> = _klineFlow.asSharedFlow()

    fun connect() {
        if (webSocket != null) return // already connecting/connected, no-op
        manuallyClosed.set(0)
        reconnectAttempts.set(0)
        openSocket()
    }

    fun disconnect() {
        manuallyClosed.set(1)
        reconnectJob?.cancel()
        heartbeatJob?.cancel()
        webSocket?.close(1000, "client_disconnect")
        webSocket = null
        _connectionState.value = ConnectionState.Disconnected
    }

    fun release() {
        disconnect()
        scope.cancel()
    }

    private fun openSocket() {
        _connectionState.value = if (reconnectAttempts.get() == 0) ConnectionState.Connecting
        else ConnectionState.Reconnecting(reconnectAttempts.get(), 0L)
        val request = Request.Builder().url(wsUrl).build()
        webSocket = okHttpClient.newWebSocket(request, listener)
    }

    private val listener = object : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            reconnectAttempts.set(0)
            lastPongAt.set(System.currentTimeMillis())
            _connectionState.value = ConnectionState.Connected
            subscribeAll(webSocket)
            startHeartbeat(webSocket)
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            if (text.equals("pong", ignoreCase = true)) {
                lastPongAt.set(System.currentTimeMillis())
                return
            }
            handlePayload(text)
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            webSocket.close(code, reason)
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            teardownAndMaybeReconnect()
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            _connectionState.value = ConnectionState.Failed(t)
            teardownAndMaybeReconnect()
        }
    }

    private fun teardownAndMaybeReconnect() {
        heartbeatJob?.cancel()
        webSocket = null
        if (manuallyClosed.get() == 1) {
            _connectionState.value = ConnectionState.Disconnected
            return
        }
        scheduleReconnect()
    }

    private fun scheduleReconnect() {
        reconnectJob?.cancel()
        val attempt = reconnectAttempts.incrementAndGet()
        val cappedAttempt = min(attempt, maxBackoffAttempts)
        val exp = (baseBackoffMs * 2.0.pow(cappedAttempt - 1)).toLong()
        val jitter = Random.nextLong(0, baseBackoffMs) // avoids thundering-herd reconnects
        val delayMs = min(exp + jitter, maxBackoffMs)
        _connectionState.value = ConnectionState.Reconnecting(attempt, delayMs)
        reconnectJob = scope.launch {
            delay(delayMs)
            if (isActive && manuallyClosed.get() == 0) openSocket()
        }
    }

    private fun startHeartbeat(socket: WebSocket) {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (isActive) {
                delay(pingIntervalMs)
                socket.send("ping")
                delay(pongTimeoutMs)
                val silentFor = System.currentTimeMillis() - lastPongAt.get()
                if (silentFor > pingIntervalMs + pongTimeoutMs) {
                    // no pong received in time; force-close to trigger onFailure/onClosed -> reconnect path
                    socket.cancel()
                    break
                }
            }
        }
    }

    private fun subscribeAll(socket: WebSocket) {
        val args = channels.map { BitgetArg(instType = instType, channel = it.wireName, instId = instId) }
        val request = BitgetSubRequest(op = "subscribe", args = args)
        socket.send(json.encodeToString(BitgetSubRequest.serializer(), request))
    }

    private fun handlePayload(text: String) {
        val envelope = runCatching { json.decodeFromString(BitgetEnvelope.serializer(), text) }.getOrNull() ?: return
        if (envelope.event == "error") return // subscription/control errors surfaced via arg==null, silently dropped here
        val arg = envelope.arg ?: return
        val dataElement = envelope.data ?: return
        when {
            arg.channel.startsWith("books") -> {
                val wireList = runCatching {
                    json.decodeFromJsonElement<List<OrderBookWire>>(dataElement)
                }.getOrNull() ?: return
                val isSnapshot = envelope.action == "snapshot"
                wireList.firstOrNull()?.let { _orderBookFlow.value = it.toSnapshot(arg.instId, isSnapshot) }
            }
            arg.channel == "ticker" -> {
                val wireList = runCatching {
                    json.decodeFromJsonElement<List<TickerWire>>(dataElement)
                }.getOrNull() ?: return
                wireList.firstOrNull()?.let { _tickerFlow.value = it.toTickerData() }
            }
            arg.channel.startsWith("candle") -> {
                val rows = runCatching {
                    json.decodeFromJsonElement<List<List<String>>>(dataElement)
                }.getOrNull() ?: return
                scope.launch { _klineFlow.emit(rows.toKlineEntries()) }
            }
        }
    }
}
