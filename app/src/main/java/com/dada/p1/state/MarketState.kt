package com.dada.p1.state

import com.dada.p1.network.dto.KlineEntry
import com.dada.p1.network.dto.TickerData

// Lifecycle of the currently selected timeframe's live feed
enum class MarketStatus { IDLE, LOADING, LIVE }

// candles is oldest-first and never exceeds MarketStateMachine.MAX_CANDLES; nothing here is persisted to disk/DB
data class MarketState(
    val timeframe: TimeFrame = TimeFrame.ONE_MINUTE,
    val candles: List<KlineEntry> = emptyList(),
    val ticker: TickerData? = null,
    val status: MarketStatus = MarketStatus.IDLE
)
