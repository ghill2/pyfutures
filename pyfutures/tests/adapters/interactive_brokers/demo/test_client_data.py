import asyncio

import pytest
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract
from ibapi.contract import Contract as IBContract

from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow


class TestInteractiveBrokersClientData:
    @pytest.mark.asyncio()
    async def test_request_head_timestamp_single(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        timestamp = await client.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID,
        )
        assert str(timestamp) == "2022-03-29 08:00:00+00:00"

    @pytest.mark.asyncio()
    async def test_request_quote_ticks(self, client):
        contract = Contract()
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
        assert all(isinstance(quote, IBQuoteTick) for quote in quotes)

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
        assert all(isinstance(quote, IBQuoteTick) for quote in quotes)

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
        contract = Contract()
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
        contract = Contract()
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

        assert all(isinstance(bar, IBBar) for bar in bars)
        assert len(bars) > 0
