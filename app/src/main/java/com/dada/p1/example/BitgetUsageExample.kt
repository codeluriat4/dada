package com.dada.p1.example

import com.dada.p1.network.BitgetWebSocketClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

fun exampleUsage(scope: CoroutineScope) {
    val client = BitgetWebSocketClient(instId = "BTCUSDT") // defaults to books15 + ticker + candle1m
    client.connect()

    scope.launch { client.connectionState.collect { /* state -> update UI connection indicator */ } }
    scope.launch { client.orderBookFlow.collect { /* snapshot -> render bid/ask ladder */ } }
    scope.launch { client.tickerFlow.collect { /* ticker -> update last price / 24h stats */ } }
    scope.launch { client.klineFlow.collect { /* candles -> append to chart series */ } }

    // client.disconnect() // call when the screen/lifecycle owner is torn down
    // client.release()    // call when the client instance itself is discarded for good
}
