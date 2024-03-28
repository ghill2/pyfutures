import asyncio
import random
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.common import BarData
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.data import InteractiveBrokersDataClient
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.tests.unit.adapter.stubs import AdapterStubs
from pyfutures.tests.unit.client.stubs import ClientStubs


class TestInteractiveBrokersDataClient:
    def setup_method(self):
        clock = LiveClock()

        msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=clock,
        )

        cache = TestComponentStubs.cache()
        self.contract = AdapterStubs.contract()
        cache.add_instrument(self.contract)

        client: InteractiveBrokersClient = ClientStubs.client()

        self.data_client = InteractiveBrokersDataClient(
            loop=asyncio.get_event_loop(),
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=AdapterStubs.instrument_provider(client),
            config=InteractiveBrokersDataClientConfig(),
        )

        self.instrument_id = InstrumentId.from_str("MES=MES=FUT=2023Z.CME")
        self.bar_type = BarType.from_str(f"{self.instrument_id}-1-DAY-MID-EXTERNAL")

    @pytest.mark.skip(reason="TODO")
    def test_instruments_on_load(self):
        pass

    @pytest.mark.asyncio()
    async def test_subscribe_bars(self):
        # Arrange
        self.data_client._client.subscribe_bars = Mock()

        # Act
        await self.data_client._subscribe_bars(self.bar_type)

        # Assert
        self.data_client._client.subscribe_bars.assert_called_once()
        sent_kwargs = self.data_client._client.subscribe_bars.call_args_list[0][1]
        assert sent_kwargs["contract"].secType == "FUT"
        assert sent_kwargs["contract"].tradingClass == "MES"
        assert sent_kwargs["contract"].symbol == "MES"
        assert sent_kwargs["contract"].exchange == "CME"
        assert sent_kwargs["contract"].lastTradeDateOrContractMonth == "202312"
        assert sent_kwargs["what_to_show"] == WhatToShow.MIDPOINT
        assert sent_kwargs["bar_size"] == BarSize._1_DAY
        assert sent_kwargs["callback"].func == self.data_client._bar_callback
        assert sent_kwargs["callback"].keywords["bar_type"] == self.bar_type
        assert sent_kwargs["callback"].keywords[
            "instrument"
        ] == self.data_client.cache.instrument(self.instrument_id)

    @pytest.mark.asyncio()
    async def test_bar_callback(self):
        # Arrange
        bar = BarData()
        timestamp = pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        bar.timestamp = timestamp
        bar.date = 1700069390
        bar.open = 1.1
        bar.high = 1.2
        bar.low = 1.0
        bar.close = 1.1
        bar.volume = Decimal("1")
        bar.wap = Decimal("1")
        bar.barCount = 1
        instrument = self.data_client.cache.instrument(self.instrument_id)
        self.data_client._handle_data = Mock()

        # Act
        self.data_client._bar_callback(
            bar_type=self.bar_type,
            bar=bar,
            instrument=instrument,
        )

        # Assert
        self.data_client._handle_data.assert_called_once_with(
            Bar(
                bar_type=self.bar_type,
                open=Price(1.1, instrument.price_precision),
                high=Price(1.2, instrument.price_precision),
                low=Price(1.0, instrument.price_precision),
                close=Price(1.1, instrument.price_precision),
                volume=Quantity(1, instrument.size_precision),
                ts_init=dt_to_unix_nanos(timestamp),
                ts_event=dt_to_unix_nanos(timestamp),
            )
        )

    @pytest.mark.asyncio()
    async def test_request_bars_handles_expected(self):
        # Arrange
        request_mock = Mock(side_effect=self.request_bars)
        self.data_client._historic.request_bars = request_mock
        start = pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        now = pd.Timestamp("2023-11-16", tz="UTC")
        pd.Timestamp.utcnow = Mock(return_value=now)
        correlation_id = UUID4()
        handle_mock = Mock()
        self.data_client._handle_bars = handle_mock

        # Act
        await self.data_client._request_bars(
            bar_type=self.bar_type,
            limit=0,
            correlation_id=correlation_id,
            start=start,
        )

        # Assert
        request_mock.assert_called_once()
        sent_kwargs = request_mock.call_args_list[0][1]
        assert sent_kwargs["contract"].secType == "FUT"
        assert sent_kwargs["contract"].tradingClass == "MES"
        assert sent_kwargs["contract"].symbol == "MES"
        assert sent_kwargs["contract"].exchange == "CME"
        assert sent_kwargs["contract"].lastTradeDateOrContractMonth == "202312"
        assert sent_kwargs["what_to_show"] == WhatToShow.MIDPOINT
        assert sent_kwargs["bar_size"] == BarSize._1_DAY
        assert sent_kwargs["start_time"] == start
        assert sent_kwargs["end_time"] == now
        assert sent_kwargs["limit"] is None

        handle_mock.assert_called_once()
        sent_kwargs = handle_mock.call_args_list[0][1]
        assert sent_kwargs["bar_type"] == self.bar_type
        assert len(sent_kwargs["bars"]) == 30
        assert all(isinstance(b, Bar) for b in sent_kwargs["bars"])
        assert sent_kwargs["partial"] is None
        assert sent_kwargs["correlation_id"] == correlation_id

    async def request_bars(self, **kwargs) -> list[BarData]:
        end_time = kwargs["end_time"]
        start_time = end_time - pd.Timedelta(hours=24)
        time_range = ((end_time - start_time).total_seconds() / 60) - 1

        random_times = set()
        while len(random_times) != 30:
            random_minute = random.randint(0, time_range)
            random_time = start_time + pd.Timedelta(minutes=random_minute)
            assert random_time >= start_time and random_time < end_time
            random_times.add(random_time)

        bars = [BarData() for _ in range(30)]
        for i, time in enumerate(random_times):
            bars[i].timestamp = time
            bars[i].date = int(time.timestamp())
            bars[i].open = 1.1
            bars[i].high = 1.2
            bars[i].low = 1.0
            bars[i].close = 1.1
            bars[i].volume = Decimal("1")
            bars[i].wap = Decimal("1")
            bars[i].barCount = 1
        return bars


# # InteractiveBrokersDataClient Tests
# import asyncio

# import pytest
# from nautilus_trader.common.component import init_logging
# from nautilus_trader.common.enums import LogLevel

# from nautilus_trader.model.data import BarType

# from pyfutures.adapter.config import (
#     InteractiveBrokersDataClientConfig,
# )
# from pyfutures.adapter.config import (
#     InteractiveBrokersInstrumentProviderConfig,
# )
# from pyfutures.tests.adapters.interactive_brokers.demo.factories import (
#     InteractiveBrokersDataEngineFactory,
# )
# from pyfutures.tests.test_kit import IBTestProviderStubs


# init_logging(level_stdout=LogLevel.DEBUG)


# from ibapi.contract import Contract as IBContract


# def test_data_universe_load_start(msgbus, cache, clock):
#     """
#     when an equivalent trading node test is setup, this test is redundant,
#     as the execution path would be tested in the trading node test
#     """
#     instrument_ids = [
#         r.instrument_id_live.value for r in IBTestProviderStubs.universe_rows()
#     ]
#     data_client_config = InteractiveBrokersDataClientConfig(
#         instrument_provider=InteractiveBrokersInstrumentProviderConfig(
#             load_ids=instrument_ids
#         )
#     )
#     data_engine, data_client = InteractiveBrokersDataEngineFactory.create(
#         msgbus, cache, clock, client_config=data_client_config
#     )
#     asyncio.get_event_loop().run_until_complete(data_client._connect())


# @pytest.mark.asyncio
# async def test_contract(client):
#     # contract = IBContract()
#     # contract.secType = "CASH"
#     # contract.exchange = "IBFX"
#     # contract.localSymbol = "AUD.CAD"
#     contract = IBContract()
#     contract.symbol = "EUR"
#     contract.currency = "GBP"
#     contract.exchange = "IDEALPRO"
#     contract.secType = "CASH"

#     await client.connect()

#     expected_contract = await client.request_contract_details(contract)
#     print(expected_contract)


# def test_data_forex_load_start(client, msgbus, cache, clock):
#     """
#     localSymbol or Symbol is required in an unqualified to obtain a qualified contract using request_contract_details()
#     Example EURGBP Forex Contract:
#         symbol = "EUR"
#         tradingClass = "EUR.GBP"
#         currenct = "GBP"

#     """
#     instrument_id = "EUR.GBP=CASH.IDEALPRO"
#     data_client_config = InteractiveBrokersDataClientConfig(
#         instrument_provider=InteractiveBrokersInstrumentProviderConfig(
#             load_ids=[instrument_id]
#         )
#     )
#     data_engine, data_client = InteractiveBrokersDataEngineFactory.create(
#         msgbus, cache, clock, client_config=data_client_config
#     )
#     asyncio.get_event_loop().run_until_complete(data_client._connect())
#     bar_type = BarType.from_str(f"{instrument_id}-5-SECOND-BID-EXTERNAL")
#     asyncio.get_event_loop().run_until_complete(
#         data_client._subscribe_bars(bar_type=bar_type)
#     )
