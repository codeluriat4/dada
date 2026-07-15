package com.dada.p1.network

sealed class ConnectionState {
    data object Idle : ConnectionState()
    data object Connecting : ConnectionState()
    data object Connected : ConnectionState()
    data class Reconnecting(val attempt: Int, val delayMs: Long) : ConnectionState()
    data class Failed(val throwable: Throwable) : ConnectionState()
    data object Disconnected : ConnectionState()
}

// Channels supported by this wrapper; extend as needed
sealed class BitgetChannel(val wireName: String) {
    data class OrderBook(val depth: Int = 15) : BitgetChannel("books$depth") // books5/books15/books
    data object Ticker : BitgetChannel("ticker")
    data class Kline(val interval: String = "1m") : BitgetChannel("candle$interval")
}
