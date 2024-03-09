import asyncio

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

# from pyfutures.adapter..client.objects import IBTradeTick
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.enums import Frequency
from pyfutures.adapter.enums import WhatToShow


init_logging(level_stdout=LogLevel.DEBUG)


class TestInteractiveBrokersClientData:
    @pytest.mark.asyncio()
    async def test_request_head_timestamp_single(self, client):
        contract = IBContract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        timestamp = await client.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID,
        )
        assert str(timestamp) == "2022-03-29 08:00:00+00:00"

    @pytest.mark.asyncio()
    async def test_request_quote_ticks(self, client):
        contract = IBContract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        quotes = await asyncio.wait_for(
            client.request_quote_ticks(
                name="test",
                contract=contract,
                count=50,
            ),
            2,
        )

        assert len(quotes) == 54
        assert all(isinstance(quote, HistoricalTickBidAsk) for quote in quotes)

    @pytest.mark.asyncio()
    async def test_request_quote_ticks_dc(self, client):
        contract = IBContract()
        contract.tradingClass = "DC"
        contract.symbol = "DA"
        contract.exchange = "CME"
        contract.secType = "CONTFUT"

        quotes = await asyncio.wait_for(
            client.request_quote_ticks(
                name="test",
                contract=contract,
                count=50,
            ),
            2,
        )

        assert len(quotes) == 54
        assert all(isinstance(quote, HistoricalTickBidAsk) for quote in quotes)

    @pytest.mark.asyncio()
    async def test_request_first_quote_tick(self, client):
        # TODO: not first timestamp for CONTFUT
        pass

    @pytest.mark.asyncio()
    async def test_request_last_quote_tick(self, client):
        contract = IBContract()
        contract.tradingClass = "DC"
        contract.symbol = "DA"
        contract.exchange = "CME"
        contract.secType = "CONTFUT"

        await client.connect()
        last = await asyncio.wait_for(
            client.request_last_quote_tick(
                contract=contract,
            ),
            2,
        )
        assert isinstance(last, HistoricalTickBidAsk)

    @pytest.mark.skip(reason="trade ticks return 0 for this contract")
    @pytest.mark.asyncio()
    async def test_request_trade_ticks(self, client):
        contract = IBContract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        trades = await asyncio.wait_for(
            client.request_trade_ticks(
                name="test",
                contract=contract,
                count=50,
            ),
            2,
        )

        assert len(trades) == 51
        assert all(isinstance(trade, IBTradeTick) for trade in trades)

    @pytest.mark.asyncio()
    async def test_request_bars(self, client):
        contract = IBContract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"
        await client.connect()
        client._client.reqMarketDataType(4)
        asyncio.sleep(2)
        bars = await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_DAY,
            duration=Duration(4, Frequency.DAY),
            what_to_show=WhatToShow.BID,
        )

        assert all(isinstance(bar, BarData) for bar in bars)
        assert len(bars) > 0

    @pytest.mark.asyncio()
    async def test_client_handles_errors(self, client):
        """If an error is raised within the callback responses, the client should show the errors in the log"""
        await client.connect()
        #
        # def side_effect(**kwargs):
        # client.historicalData(reqId=1, bar=BarData())

        # send_mock = Mock(side_effect=side_effect)
        # client._client.reqHistoricalData = send_mock
        # error_mock()
        #
        contract = IBContract()
        contract.secType = "CONTFUT"
        contract.exchange = "CME"
        contract.symbol = "DA"

        await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.TRADES,
            duration=Duration(step=1, freq=Frequency.DAY),
            end_time=pd.Timestamp.utcnow() - pd.Timedelta(days=1).floor("1D"),
        )

    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks(self, client):
        await client.connect()

        callback_mock = Mock()

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_quote_ticks(
            name="test",
            contract=contract,
            callback=callback_mock,
        )

        async def wait_for_quote_tick():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_quote_tick(), 10)

        assert callback_mock.call_count > 0
