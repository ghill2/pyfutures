import tempfile
from unittest.mock import AsyncMock
from decimal import Decimal

import pytest


import logging
import pytest
import sys

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.historic import InteractiveBrokersHistoric
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from pyfutures.tests.unit.client.mock_server import MockServer
from pyfutures.client.connection import Connection
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.tests.unit.adapter.stubs.identifiers import IBTestIdStubs
from pyfutures.adapter.factories import PROVIDER_CONFIG
from pyfutures.adapter.factories import InteractiveBrokersLiveDataClientFactory
from pyfutures.adapter.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.adapter.data import InteractiveBrokersDataClient
from pyfutures.adapter.execution import InteractiveBrokersExecClient
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider

from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import InstrumentClass
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity

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
def historic(event_loop) -> InteractiveBrokersHistoric:
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
        log_level=logging.DEBUG,
        api_log_level=logging.DEBUG,
        request_timeout_seconds=0.5,  # requests should fail immediately for unit tests
        override_timeout=True,  # use timeout for all requests even if timeout is given
    )
    # cache_dir = Path(tempfile.mkdtemp())
    historic = InteractiveBrokersHistoric(
        client=client,
        cachedir=tempfile.gettempdir(),
    )
    init_logging(level_stdout=LogLevel.DEBUG)
    
    yield historic
    # shutil.deletecache_dir.unlink()


# @pytest.fixture
# def connection(event_loop) -> Connection:
#     logger = logging.getLogger()
#     logger.setLevel(logging.DEBUG)
#     return Connection(
#         loop=event_loop,
#         host="127.0.0.1",
#         port=4002,
#         client_id=1,
#     )

@pytest.fixture()
def exec_client(event_loop, client) -> InteractiveBrokersExecClient:
    
    init_logging(level_stdout=LogLevel.DEBUG)
    
    clock = LiveClock()

    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
    )
    
    cache = TestComponentStubs.cache()
    
    exec_client = InteractiveBrokersExecClient(
        loop=event_loop,
        client=client,
        account_id=AccountId("IB-1234567"),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=InteractiveBrokersInstrumentProvider(
            client=client,
            config=PROVIDER_CONFIG,
        ),
    )
    
    contract = _mes_contract()
    cache.add_instrument(contract)
    
    exec_client.client.request_next_order_id = AsyncMock(return_value=IBTestIdStubs.orderId())
    
    yield exec_client
    
@pytest.fixture()
def data_client(event_loop, client) -> InteractiveBrokersDataClient:
    
    init_logging(level_stdout=LogLevel.DEBUG)
    
    clock = LiveClock()

    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
    )
    
    cache = TestComponentStubs.cache()
    
    data_client = InteractiveBrokersDataClient(
        loop=event_loop,
        client=client,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=InteractiveBrokersInstrumentProvider(
            client=client,
            config=PROVIDER_CONFIG,
        ),
        config=InteractiveBrokersDataClientConfig(),
    )
    
    contract = _mes_contract()
    # provider.add(contract)
    cache.add_instrument(contract)
    
    yield data_client
        
# @pytest.fixture
# def mock_socket() -> MockServer:
#     return MockServer()

def _mes_contract() -> FuturesContract:
    return Instrument(
        instrument_id=InstrumentId.from_str("MES=MES=FUT=2023Z.CME"),
        raw_symbol=Symbol("MES"),
        asset_class=AssetClass.COMMODITY,
        instrument_class=InstrumentClass.SPOT,
        quote_currency=Currency.from_str("GBP"),
        is_inverse=False,
        price_precision=4,
        size_precision=0,
        size_increment=Quantity.from_int(1),
        multiplier=Quantity.from_int(1),
        margin_init=Decimal("1"),
        margin_maint=Decimal("1"),
        maker_fee=Decimal("1"),
        taker_fee=Decimal("1"),
        ts_event=0,
        ts_init=0,
        info=dict(
            contract=dict(
                conId=1,
                exchange="CME",
            ),
        ),
    )


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