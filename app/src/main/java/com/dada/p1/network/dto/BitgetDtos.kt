package com.dada.p1.network.dto

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

// Top-level envelope common to every Bitget v2 public WS push message
@Serializable
data class BitgetEnvelope(
    val action: String? = null, // "snapshot" | "update", null for event acks
    val arg: BitgetArg? = null,
    val data: JsonElement? = null,
    val ts: Long? = null,
    val event: String? = null, // "subscribe" | "error" on control frames
    val code: Int? = null,
    val msg: String? = null
)

@Serializable
data class BitgetArg(
    val instType: String, // "SPOT"
    val channel: String,  // "books15" | "ticker" | "candle1m" ...
    val instId: String    // "BTCUSDT"
)

// Raw order book payload as sent on the wire: price/size pairs as string arrays
@Serializable
data class OrderBookWire(
    val asks: List<List<String>> = emptyList(),
    val bids: List<List<String>> = emptyList(),
    val checksum: Long? = null,
    val ts: String
)

// Optimized numeric representation, converted once at parse time to avoid repeated string->double churn downstream
data class PriceLevel(val price: Double, val size: Double)

data class OrderBookSnapshot(
    val instId: String,
    val isSnapshot: Boolean, // true for "snapshot" action, false for incremental "update"
    val asks: List<PriceLevel>,
    val bids: List<PriceLevel>,
    val checksum: Long?,
    val ts: Long
)

fun OrderBookWire.toSnapshot(instId: String, isSnapshot: Boolean): OrderBookSnapshot {
    // Bitget sends [price, size] string pairs; size == "0" signals a level removal on incremental updates
    fun List<List<String>>.toLevels() = map { PriceLevel(it[0].toDouble(), it[1].toDouble()) }
    return OrderBookSnapshot(
        instId = instId,
        isSnapshot = isSnapshot,
        asks = asks.toLevels(),
        bids = bids.toLevels(),
        checksum = checksum,
        ts = ts.toLong()
    )
}

// Ticker channel payload (subset of fields commonly needed)
@Serializable
data class TickerWire(
    val instId: String,
    val lastPr: String,
    val bidPr: String? = null,
    val askPr: String? = null,
    val bidSz: String? = null,
    val askSz: String? = null,
    val open24h: String? = null,
    val high24h: String? = null,
    val low24h: String? = null,
    val change24h: String? = null,
    val baseVolume: String? = null,
    val quoteVolume: String? = null,
    val ts: String
)

data class TickerData(
    val instId: String,
    val lastPrice: Double,
    val bidPrice: Double?,
    val askPrice: Double?,
    val high24h: Double?,
    val low24h: Double?,
    val change24hPct: Double?,
    val baseVolume: Double?,
    val quoteVolume: Double?,
    val ts: Long
)

fun TickerWire.toTickerData() = TickerData(
    instId = instId,
    lastPrice = lastPr.toDouble(),
    bidPrice = bidPr?.toDoubleOrNull(),
    askPrice = askPr?.toDoubleOrNull(),
    high24h = high24h?.toDoubleOrNull(),
    low24h = low24h?.toDoubleOrNull(),
    change24hPct = change24h?.toDoubleOrNull(),
    baseVolume = baseVolume?.toDoubleOrNull(),
    quoteVolume = quoteVolume?.toDoubleOrNull(),
    ts = ts.toLong()
)

// Kline/candle channel: each element is [ts, open, high, low, close, baseVolume, quoteVolume, usdtVolume]
data class KlineEntry(
    val ts: Long,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val baseVolume: Double,
    val quoteVolume: Double
)

fun List<List<String>>.toKlineEntries(): List<KlineEntry> = map { row ->
    KlineEntry(
        ts = row[0].toLong(),
        open = row[1].toDouble(),
        high = row[2].toDouble(),
        low = row[3].toDouble(),
        close = row[4].toDouble(),
        baseVolume = row[5].toDouble(),
        quoteVolume = row.getOrNull(6)?.toDoubleOrNull() ?: 0.0
    )
}

// Outbound subscribe/unsubscribe control frame
@Serializable
data class BitgetSubRequest(
    val op: String, // "subscribe" | "unsubscribe"
    val args: List<BitgetArg>
)
