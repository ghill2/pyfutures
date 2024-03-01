# InteractiveBrokersDataClient Tests
import asyncio

import pytest
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

from nautilus_trader.model.data import BarType

from pyfutures.adapters.interactive_brokers.config import (
    InteractiveBrokersDataClientConfig,
)
from pyfutures.adapters.interactive_brokers.config import (
    InteractiveBrokersInstrumentProviderConfig,
)
from pyfutures.tests.adapters.interactive_brokers.demo.factories import (
    InteractiveBrokersDataEngineFactory,
)
from pyfutures.tests.test_kit import IBTestProviderStubs


init_logging(level_stdout=LogLevel.DEBUG)


from ibapi.contract import Contract as IBContract


def test_data_universe_load_start(msgbus, cache, clock):
    """
    when an equivalent trading node test is setup, this test is redundant,
    as the execution path would be tested in the trading node test
    """
    instrument_ids = [
        r.instrument_id_live.value for r in IBTestProviderStubs.universe_rows()
    ]
    data_client_config = InteractiveBrokersDataClientConfig(
        instrument_provider=InteractiveBrokersInstrumentProviderConfig(
            load_ids=instrument_ids
        )
    )
    data_engine, data_client = InteractiveBrokersDataEngineFactory.create(
        msgbus, cache, clock, client_config=data_client_config
    )
    asyncio.get_event_loop().run_until_complete(data_client._connect())


@pytest.mark.asyncio
async def test_contract(client):
    # contract = IBContract()
    # contract.secType = "CASH"
    # contract.exchange = "IBFX"
    # contract.localSymbol = "AUD.CAD"
    contract = IBContract()
    contract.symbol = "EUR"
    contract.currency = "GBP"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"

    await client.connect()

    expected_contract = await client.request_contract_details(contract)
    print(expected_contract)


def test_data_forex_load_start(client, msgbus, cache, clock):
    """
    localSymbol or Symbol is required in an unqualified to obtain a qualified contract using request_contract_details()
    Example EURGBP Forex Contract:
        symbol = "EUR"
        tradingClass = "EUR.GBP"
        currenct = "GBP"

    """
    instrument_id = "EUR.GBP=CASH.IDEALPRO"
    data_client_config = InteractiveBrokersDataClientConfig(
        instrument_provider=InteractiveBrokersInstrumentProviderConfig(
            load_ids=[instrument_id]
        )
    )
    data_engine, data_client = InteractiveBrokersDataEngineFactory.create(
        msgbus, cache, clock, client_config=data_client_config
    )
    asyncio.get_event_loop().run_until_complete(data_client._connect())
    bar_type = BarType.from_str(f"{instrument_id}-5-SECOND-BID-EXTERNAL")
    asyncio.get_event_loop().run_until_complete(
        data_client._subscribe_bars(bar_type=bar_type)
    )
