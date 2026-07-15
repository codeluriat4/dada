package com.dada.p1.state

import com.dada.p1.network.dto.KlineEntry

// (state, event) -> state. Deterministic and side-effect free so behavior can be verified without mocks
object MarketStateMachine {
    const val MAX_CANDLES = 100 // hard cap, in-memory only, enforced on every kline event

    fun reduce(state: MarketState, event: MarketEvent): MarketState = when (event) {
        is MarketEvent.TimeframeSelected -> onTimeframeSelected(state, event.timeframe)
        is MarketEvent.KlineReceived -> onKlineReceived(state, event)
        is MarketEvent.TickerReceived -> state.copy(ticker = event.ticker)
        MarketEvent.ConnectionLost -> state.copy(status = MarketStatus.IDLE)
        MarketEvent.Reset -> MarketState(timeframe = state.timeframe)
    }

    private fun onTimeframeSelected(state: MarketState, timeframe: TimeFrame): MarketState {
        if (timeframe == state.timeframe && state.candles.isNotEmpty()) return state // already selected and loaded, no-op
        return MarketState(timeframe = timeframe, status = MarketStatus.LOADING) // fresh instance: old candle list becomes unreachable/GC-eligible
    }

    private fun onKlineReceived(state: MarketState, event: MarketEvent.KlineReceived): MarketState {
        if (event.timeframe != state.timeframe) return state // stale push from an abandoned channel/timeframe, dropped
        var window = state.candles
        for (candle in event.candles) window = upsert(window, candle)
        return state.copy(candles = window, status = MarketStatus.LIVE)
    }

    // Bitget re-pushes the still-forming candle repeatedly under the same ts; only a new ts is a genuinely new candle
    private fun upsert(window: List<KlineEntry>, candle: KlineEntry): List<KlineEntry> {
        if (window.isEmpty()) return listOf(candle)
        val last = window.last()
        if (last.ts == candle.ts) return window.dropLast(1) + candle // in-place refresh, size unchanged
        val grown = window + candle
        return if (grown.size > MAX_CANDLES) grown.subList(grown.size - MAX_CANDLES, grown.size).toList() else grown // append then evict oldest
    }
}
