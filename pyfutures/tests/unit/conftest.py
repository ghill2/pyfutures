import asyncio
import json
from pathlib import Path

import pytest

from pyfutures.adapter.execution import InteractiveBrokersExecutionClient
from pyfutures.adapter.parsing import dict_to_contract_details
from pyfutures.tests.demo.adapter.factories import InteractiveBrokersExecEngineFactory
import logging
import pytest
import sys

from pyfutures.adapter.client.client import InteractiveBrokersClient
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from pyfutures.tests.unit.client.mock_socket import MockSocket
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.adapter.client.connection import Connection
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from unittest.mock import AsyncMock
from nautilus_trader.adapters.interactive_brokers.execution import InteractiveBrokersExecutionClient
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.tests.demo.adapter.factories import InteractiveBrokersExecEngineFactory
from pyfutures.tests.unit.adapter.stubs.identifiers import IBTestIdStubs

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

@pytest.fixture
def client(event_loop) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
        log_level=logging.DEBUG,
        api_log_level=logging.DEBUG,
        request_timeout_seconds=0.5,  # requests should fail immediately for unit tests
        override_timeout=True,  # use timeout for all requests even if timeout is given
    )
    return client


@pytest.fixture
def connection(event_loop) -> Connection:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return Connection(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
    )

@pytest.fixture()
def exec_client(event_loop) -> InteractiveBrokersExecutionClient:
    
    init_logging(level_stdout=LogLevel.DEBUG)
    
    clock = LiveClock()

    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
    )
    
    cache = TestComponentStubs.cache()
    
    exec_engine, exec_client, provider, client  = InteractiveBrokersExecEngineFactory.create(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    
    contract = IBTestProviderStubs.mes_contract()
    provider.add(contract)
    cache.add_instrument(contract)
    
    client.request_next_order_id = AsyncMock(return_value=IBTestIdStubs.orderId())
    
    yield exec_client
    
        
@pytest.fixture
def mock_socket() -> MockSocket:
    return MockSocket()


# def _load_contracts(instrument_provider):

#     load_ids = {
#         "D[X23].ICEEUSOFT",  # near expired contract
#         "MES[Z23].CME",  # US liquid contract
#         "R[Z23].ICEEU",  # UK liquid contract
#         "MDAX[Z23].EUREX",  # UK illiquid contract
#         "QM[Z23].NYMEX",  # US illiquid contract
#         # "QM[F23].NYMEX",  # US illiquid contract
#         # DA, IXV, ALI  # US illiquid contracts
#     }

#     folder = Path(__file__).parent.parent / "responses/import_contracts"
#     for instrument_id in load_ids:
#         file = folder / (instrument_id + ".json")
#         assert file.exists()

#         instrument_provider.add_contract_details(
#             dict_to_contract_details(json.loads(file.read_text())),
#         )

#     # check contracts are loaded
#     for instrument_id in load_ids:
#         assert instrument_provider.find(InstrumentId.from_str(instrument_id)) is not None