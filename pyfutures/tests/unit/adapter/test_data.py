import pandas as pd
import pytest
from unittest.mock import Mock

from decimal import Decimal

from nautilus_trader.model.data import BarType
from ibapi.common import BarData
from pyfutures.adapter.enums import BarSize
from nautilus_trader.core.datetime import dt_to_unix_nanos
from pyfutures.adapter.enums import WhatToShow
from ibapi.contract import Contract as IBContract
import functools
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from pyfutures.adapter.parsing import instrument_id_to_contract
from nautilus_trader.model.data import Bar

class TestInteractiveBrokersDataClient:
    
    def setup_method(self):
        self.instrument_id = InstrumentId.from_str("MES=MES=FUT=2023Z.CME")
        self.bar_type = BarType.from_str(f"{self.instrument_id}-1-DAY-MID-EXTERNAL")
        
    @pytest.mark.skip(reason="TODO")
    def test_instruments_on_load(self, data_client):
        pass
    
    @pytest.mark.asyncio()
    async def test_subscribe_bars(self, data_client):
        
        # Arrange
        data_client._client.subscribe_bars = Mock()
        
        # Act
        await data_client._subscribe_bars(self.bar_type)
        
        # Assert
        data_client._client.subscribe_bars.assert_called_once()
        sent_kwargs = data_client._client.subscribe_bars.call_args_list[0][1]
        sent_kwargs["contract"] == instrument_id_to_contract(self.instrument_id)
        sent_kwargs["what_to_show"] == WhatToShow.BID_ASK
        sent_kwargs["bar_size"] == BarSize._1_DAY
        sent_kwargs["callback"] == functools.partial(
            data_client._bar_callback,
            bar_type=self.bar_type,
            instrument=data_client.cache.instrument(self.instrument_id),
        )
            
    @pytest.mark.asyncio()
    async def test_bar_callback(self, data_client):
        
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
        instrument = data_client.cache.instrument(self.instrument_id)
        data_client._handle_data = Mock()
        
        data_client._bar_callback(
            bar_type=self.bar_type,
            bar=bar,
            instrument=instrument,
        )
        
        data_client._handle_data.assert_called_once_with(
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
