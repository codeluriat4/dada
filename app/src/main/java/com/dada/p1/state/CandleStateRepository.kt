package com.dada.p1.state

import com.dada.p1.network.BitgetWebSocketClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

// No local database anywhere: the only state is this in-memory StateFlow, capped by MarketStateMachine.MAX_CANDLES
class CandleStateRepository(
    private val scope: CoroutineScope,
    private val clientFactory: (TimeFrame) -> BitgetWebSocketClient, // one fresh socket per timeframe switch
    initialTimeframe: TimeFrame = TimeFrame.ONE_MINUTE
) {
    private val _state = MutableStateFlow(MarketState(timeframe = initialTimeframe))
    val state: StateFlow<MarketState> = _state.asStateFlow()

    private var activeClient: BitgetWebSocketClient? = null
    private var klineJob: Job? = null
    private var tickerJob: Job? = null

    init {
        rewire(initialTimeframe)
    }

    fun selectTimeframe(timeframe: TimeFrame) {
        dispatch(MarketEvent.TimeframeSelected(timeframe))
        rewire(timeframe)
    }

    // Tears down the previous socket + collectors completely before standing up the new ones: nothing old survives the switch
    private fun rewire(timeframe: TimeFrame) {
        klineJob?.cancel()
        tickerJob?.cancel()
        activeClient?.release()

        val client = clientFactory(timeframe)
        activeClient = client
        client.connect()

        klineJob = scope.launch {
            client.klineFlow.collect { entries -> dispatch(MarketEvent.KlineReceived(timeframe, entries)) }
        }
        tickerJob = scope.launch {
            client.tickerFlow.collect { ticker -> ticker?.let { dispatch(MarketEvent.TickerReceived(it)) } }
        }
    }

    fun dispatch(event: MarketEvent) {
        _state.update { MarketStateMachine.reduce(it, event) }
    }

    // Full teardown for when the owner (ViewModel/screen) is destroyed; releases the socket and stops all collection
    fun release() {
        klineJob?.cancel()
        tickerJob?.cancel()
        activeClient?.release()
        activeClient = null
    }
}
