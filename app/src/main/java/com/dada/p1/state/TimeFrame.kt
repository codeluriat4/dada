package com.dada.p1.state

import com.dada.p1.network.BitgetChannel

// wireInterval must match Bitget's candleX channel suffix exactly; note hour granularity uses uppercase "H" on the wire
enum class TimeFrame(val label: String, val wireInterval: String) {
    ONE_MINUTE("1m", "1m"),
    FIVE_MINUTES("5m", "5m"),
    FIFTEEN_MINUTES("15m", "15m"),
    THIRTY_MINUTES("30m", "30m"),
    ONE_HOUR("1h", "1H");

    val channel: BitgetChannel get() = BitgetChannel.Kline(wireInterval)
}
