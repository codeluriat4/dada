package com.dada.p1.state

import com.dada.p1.network.dto.KlineEntry
import com.dada.p1.network.dto.TickerData
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class MarketStateMachineTest {

    private fun candle(ts: Long, close: Double = ts.toDouble()) =
        KlineEntry(ts = ts, open = close, high = close, low = close, close = close, baseVolume = 1.0, quoteVolume = 1.0)

    @Test
    fun `appending fewer than 100 candles keeps them all`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        repeat(40) { i -> state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(i.toLong())))) }
        assertEquals(40, state.candles.size)
    }

    @Test
    fun `exactly 100 candles are kept once the cap is crossed`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        repeat(250) { i -> state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(i.toLong())))) }
        assertEquals(100, state.candles.size)
    }

    @Test
    fun `oldest candle is discarded first, FIFO`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        repeat(150) { i -> state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(i.toLong())))) }
        assertEquals(50L, state.candles.first().ts) // candles 0..49 evicted, 50..149 remain
        assertEquals(149L, state.candles.last().ts)
    }

    @Test
    fun `repeated update to the still-forming candle does not grow the window`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(1L, close = 10.0))))
        repeat(20) { i -> state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(1L, close = 10.0 + i)))) }
        assertEquals(1, state.candles.size)
        assertEquals(29.0, state.candles.first().close, 0.0001)
    }

    @Test
    fun `batched candles within one event are each upserted in order`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        val batch = listOf(candle(1L), candle(2L), candle(2L, close = 99.0), candle(3L))
        state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, batch))
        assertEquals(3, state.candles.size)
        assertEquals(99.0, state.candles[1].close, 0.0001)
    }

    @Test
    fun `100-candle cap holds under a mix of same-ts refreshes and new candles`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        var ts = 0L
        repeat(300) { i ->
            if (i % 3 != 0) { /* refresh current candle */ } else ts++
            state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(ts))))
            assertTrue(state.candles.size <= MarketStateMachine.MAX_CANDLES) // invariant must hold after every single event
        }
        assertEquals(100, state.candles.size)
    }

    @Test
    fun `kline event for a different timeframe than the current selection is ignored`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.FIVE_MINUTES, listOf(candle(1L))))
        assertTrue(state.candles.isEmpty())
    }

    @Test
    fun `selecting a new timeframe clears the candle window entirely`() {
        var state = MarketState(timeframe = TimeFrame.ONE_MINUTE)
        repeat(100) { i -> state = MarketStateMachine.reduce(state, MarketEvent.KlineReceived(TimeFrame.ONE_MINUTE, listOf(candle(i.toLong())))) }
        assertEquals(100, state.candles.size)

        state = MarketStateMachine.reduce(state, MarketEvent.TimeframeSelected(TimeFrame.FIVE_MINUTES))
        assertEquals(TimeFrame.FIVE_MINUTES, state.timeframe)
        assertTrue(state.candles.isEmpty())
        assertEquals(MarketStatus.LOADING, state.status)
    }

    @Test
    fun `timeframe rebuild produces a fresh state instance, not a mutation of the old one`() {
        val before = MarketState(timeframe = TimeFrame.ONE_MINUTE, candles = listOf(candle(1L)))
        val after = MarketStateMachine.reduce(before, MarketEvent.TimeframeSelected(TimeFrame.FIFTEEN_MINUTES))
        assertNotSame(before, after)
        assertNotSame(before.candles, after.candles)
        assertEquals(1, before.candles.size) // original snapshot untouched: no shared mutable list leaking across timeframes
    }

    @Test
    fun `reselecting the same already-loaded timeframe is a no-op`() {
        val loaded = MarketState(timeframe = TimeFrame.ONE_MINUTE, candles = listOf(candle(1L)), status = MarketStatus.LIVE)
        val result = MarketStateMachine.reduce(loaded, MarketEvent.TimeframeSelected(TimeFrame.ONE_MINUTE))
        assertSame(loaded, result)
    }

    @Test
    fun `ticker update leaves the candle window untouched`() {
        val ticker = TickerData(
            instId = "BTCUSDT", lastPrice = 100.0, bidPrice = null, askPrice = null,
            high24h = null, low24h = null, change24hPct = null, baseVolume = null, quoteVolume = null, ts = 1L
        )
        val before = MarketState(timeframe = TimeFrame.ONE_MINUTE, candles = listOf(candle(1L)))
        val after = MarketStateMachine.reduce(before, MarketEvent.TickerReceived(ticker))
        assertEquals(before.candles, after.candles)
        assertEquals(ticker, after.ticker)
    }

    @Test
    fun `reset rebuilds an empty state but keeps the current timeframe`() {
        val before = MarketState(timeframe = TimeFrame.THIRTY_MINUTES, candles = listOf(candle(1L)), status = MarketStatus.LIVE)
        val after = MarketStateMachine.reduce(before, MarketEvent.Reset)
        assertTrue(after.candles.isEmpty())
        assertEquals(TimeFrame.THIRTY_MINUTES, after.timeframe)
    }

    @Test
    fun `all five supported timeframes carry the exact Bitget wire interval`() {
        assertEquals("1m", TimeFrame.ONE_MINUTE.wireInterval)
        assertEquals("5m", TimeFrame.FIVE_MINUTES.wireInterval)
        assertEquals("15m", TimeFrame.FIFTEEN_MINUTES.wireInterval)
        assertEquals("30m", TimeFrame.THIRTY_MINUTES.wireInterval)
        assertEquals("1H", TimeFrame.ONE_HOUR.wireInterval) // Bitget uses uppercase H for the hourly candle channel
    }
}
