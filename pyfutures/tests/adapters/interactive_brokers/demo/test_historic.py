import asyncio
import pandas as pd

import pytest
from ibapi.contract import Contract as IBContract

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import Logger
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.historic import InteractiveBrokersHistoric
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime


class TestInteractiveBrokersHistoric:
    @pytest.mark.asyncio()
    async def test_hourly(self):
        await self.client.connect()

        contract = IBContract()
        contract.conId = 452341897
        contract.exchange = "COMEX"

        # for contract in IBTestProviderStubs.universe_contracts():
        bars = await self.historic.download(
            contract=contract,
            bar_size=BarSize._1_HOUR,
            # bar_size=BarSize._5_SECOND,
            what_to_show=WhatToShow.TRADES,
        )
        assert len(bars) == 34478
        assert (
            str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"
        )

    @pytest.mark.asyncio()
    async def test_daily_downloads_expected(self):
        await self.client.connect()

        contract = IBContract()
        contract.conId = 452341897
        contract.symbol = "ALI"
        contract.exchange = "COMEX"

        # for contract in IBTestProviderStubs.universe_contracts():
        df = await self.historic.download(
            contract=contract,
            bar_size=BarSize._1_DAY,
            # bar_size=BarSize._5_SECOND,
            what_to_show=WhatToShow.TRADES,
        )
        
        # assert len(bars) == 250
        # assert str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"

    @pytest.mark.asyncio()
    async def test_request_quote_ticks_dc(self, client):
        
        historic = InteractiveBrokersHistoric(client=client)
        
        contract = IBContract()
        contract.tradingClass = "DC"
        contract.symbol = "DA"
        contract.exchange = "CME"
        contract.secType = "CONTFUT"
        
        start_time = pd.Timestamp("2023-02-13 14:30:00+00:00")
        end_time = pd.Timestamp("2023-02-13 22:00:00+00:00")
        
        await client.connect()
        quotes = await historic.request_quote_ticks(
            contract=contract,
            start_time=start_time,
            end_time=end_time,
        )
        assert all([parse_datetime(q.time) >= start_time and parse_datetime(q.time) < end_time for q in quotes])
        assert parse_datetime(quotes[0].time) == pd.Timestamp("2023-02-13 14:34:11+00:00")
        assert parse_datetime(quotes[-1].time) == pd.Timestamp("2023-02-13 21:44:26+00:00")
        
    @pytest.mark.asyncio()
    async def test_request_quote_ticks_zn(self, client):
        
        contract = IBContract()
        contract.tradingClass = "ZN"
        contract.symbol = "ZN"
        contract.exchange = "CBOT"
        contract.secType = "CONTFUT"
        start_time = pd.Timestamp("2024-01-18 17:50:00-00:00")
        end_time = pd.Timestamp("2024-01-18 18:00:00-00:00")
        historic = InteractiveBrokersHistoric(client=client, delay=2)
        
        await client.connect()
        quotes = await historic.request_quote_ticks(
            contract=contract,
            start_time=start_time,
            end_time=end_time,
        )
        for quote in quotes:
            print(parse_datetime(quote.time))
        