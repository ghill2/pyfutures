import asyncio

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


class TestInteractiveBrokersHistoric:
    def setup(self):
        clock = LiveClock()
        logger = Logger(clock, level_stdout=LogLevel.INFO)

        msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=clock,
            logger=logger,
        )
        cache = TestComponentStubs.cache()

        self.client = InteractiveBrokersClient(
            loop=asyncio.get_event_loop(),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            host="127.0.0.1",
            port=4002,
            client_id=1,
        )

        self.historic = InteractiveBrokersHistoric(client=self.client, logger=logger)

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
        print(df)
        # assert len(bars) == 250
        # assert str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"
