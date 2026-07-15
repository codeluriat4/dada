package com.dada.p1.state

import com.dada.p1.network.dto.KlineEntry
import com.dada.p1.network.dto.TickerData

sealed class MarketEvent {
    data class TimeframeSelected(val timeframe: TimeFrame) : MarketEvent()
    data class KlineReceived(val timeframe: TimeFrame, val candles: List<KlineEntry>) : MarketEvent()
    data class TickerReceived(val ticker: TickerData) : MarketEvent()
    data object ConnectionLost : MarketEvent()
    data object Reset : MarketEvent()
}
