package com.dada.p1.state

import com.dada.p1.network.BitgetWebSocketClient
import com.dada.p1.network.dto.KlineEntry
import com.dada.p1.network.dto.TickerData
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertTrue
import org.junit.Test

class CandleStateRepositoryTest {

    private fun candle(ts: Long) =
        KlineEntry(ts = ts, open = 1.0, high = 1.0, low = 1.0, close = 1.0, baseVolume = 1.0, quoteVolume = 1.0)

    // Fresh mock socket per call, each with its own independently-emittable kline/ticker flows; connect/release stay no-ops
    private fun fakeClient(): Triple<BitgetWebSocketClient, MutableSharedFlow<List<KlineEntry>>, MutableStateFlow<TickerData?>> {
        val klineFlow = MutableSharedFlow<List<KlineEntry>>(extraBufferCapacity = 64)
        val tickerFlow = MutableStateFlow<TickerData?>(null)
        val client = mockk<BitgetWebSocketClient>(relaxed = true)
        every { client.klineFlow } returns klineFlow.asSharedFlow()
        every { client.tickerFlow } returns tickerFlow.asStateFlow()
        return Triple(client, klineFlow, tickerFlow)
    }

    @Test
    fun `state caps at exactly 100 candles as updates stream in`() = runTest {
        val (client, klineFlow, _) = fakeClient()
        val repo = CandleStateRepository(scope = backgroundScope, clientFactory = { client })
        advanceUntilIdle()

        repeat(250) { i -> klineFlow.emit(listOf(candle(i.toLong()))) }
        advanceUntilIdle()

        assertEquals(100, repo.state.value.candles.size)
        assertEquals(150L, repo.state.value.candles.first().ts)
        assertEquals(249L, repo.state.value.candles.last().ts)
    }

    @Test
    fun `still-forming candle updates in place and never inflates the count past 100`() = runTest {
        val (client, klineFlow, _) = fakeClient()
        val repo = CandleStateRepository(scope = backgroundScope, clientFactory = { client })
        advanceUntilIdle()

        repeat(120) { klineFlow.emit(listOf(candle(99L))) } // exchange re-pushing the same still-open candle 120 times
        advanceUntilIdle()

        assertEquals(1, repo.state.value.candles.size)
    }

    @Test
    fun `switching timeframe releases the old socket and connects the new one`() = runTest {
        val (firstClient, _, _) = fakeClient()
        val (secondClient, _, _) = fakeClient()
        var callCount = 0
        val repo = CandleStateRepository(
            scope = backgroundScope,
            clientFactory = { callCount++; if (callCount == 1) firstClient else secondClient }
        )
        advanceUntilIdle()

        repo.selectTimeframe(TimeFrame.FIVE_MINUTES)
        advanceUntilIdle()

        verify(exactly = 1) { firstClient.release() }
        verify(exactly = 1) { secondClient.connect() }
    }

    @Test
    fun `stale emissions from a released socket cannot leak into the new timeframe's state`() = runTest {
        val (firstClient, firstKlineFlow, _) = fakeClient()
        val (secondClient, secondKlineFlow, _) = fakeClient()
        var callCount = 0
        val repo = CandleStateRepository(
            scope = backgroundScope,
            clientFactory = { callCount++; if (callCount == 1) firstClient else secondClient }
        )
        advanceUntilIdle()
        firstKlineFlow.emit(listOf(candle(1L)))
        advanceUntilIdle()
        assertEquals(1, repo.state.value.candles.size)

        repo.selectTimeframe(TimeFrame.FIVE_MINUTES)
        advanceUntilIdle()
        firstKlineFlow.emit(listOf(candle(2L))) // old collector job was cancelled on rewire, so this must go nowhere
        advanceUntilIdle()

        assertTrue(repo.state.value.candles.isEmpty())
        assertEquals(TimeFrame.FIVE_MINUTES, repo.state.value.timeframe)

        secondKlineFlow.emit(listOf(candle(3L)))
        advanceUntilIdle()
        assertEquals(1, repo.state.value.candles.size)
        assertEquals(3L, repo.state.value.candles.first().ts)
    }

    @Test
    fun `timeframe rebuild yields a distinct candle list, old snapshot stays intact for any existing holder`() = runTest {
        val (client, klineFlow, _) = fakeClient()
        val repo = CandleStateRepository(scope = backgroundScope, clientFactory = { client })
        advanceUntilIdle()
        klineFlow.emit(listOf(candle(1L)))
        advanceUntilIdle()

        val snapshotBeforeSwitch = repo.state.value
        repo.selectTimeframe(TimeFrame.ONE_HOUR)
        advanceUntilIdle()

        assertNotSame(snapshotBeforeSwitch, repo.state.value)
        assertNotSame(snapshotBeforeSwitch.candles, repo.state.value.candles)
        assertEquals(1, snapshotBeforeSwitch.candles.size) // untouched, proving no in-place mutation/leak into old references
    }

    @Test
    fun `release tears down the active socket and stops all further collection`() = runTest {
        val (client, klineFlow, _) = fakeClient()
        val repo = CandleStateRepository(scope = backgroundScope, clientFactory = { client })
        advanceUntilIdle()

        repo.release()
        advanceUntilIdle()
        klineFlow.emit(listOf(candle(1L)))
        advanceUntilIdle()

        verify(exactly = 1) { client.release() }
        assertTrue(repo.state.value.candles.isEmpty())
    }

    @Test
    fun `ticker updates from the active socket flow into state`() = runTest {
        val (client, _, tickerFlow) = fakeClient()
        val repo = CandleStateRepository(scope = backgroundScope, clientFactory = { client })
        advanceUntilIdle()

        val ticker = TickerData(
            instId = "BTCUSDT", lastPrice = 42.0, bidPrice = null, askPrice = null,
            high24h = null, low24h = null, change24hPct = null, baseVolume = null, quoteVolume = null, ts = 1L
        )
        tickerFlow.value = ticker
        advanceUntilIdle()

        assertEquals(ticker, repo.state.value.ticker)
    }
}
