import asyncio

import pytest
import pytest_asyncio
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Price
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs

# fmt: off
from pyfutures.adapter import IB_VENUE
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.execution import InteractiveBrokersExecClient
from pyfutures.adapter.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.tests.demo.order_setup import OrderSetup
import asyncio

import pytest
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.adapter.client.client import InteractiveBrokersClient


def pytest_addoption(parser):
    parser.addoption(
        '--instrument-id',
        action='store',
        # default="EOE=MFA=Z23.FTA",
        default="DC=DA=FUT=2024G.CME",
        help='Base URL for the API tests',
    )
    parser.addoption(
        '--file-logging',
        action='store',
        default=False,
        help='Enable file logging for the test',
    )
    parser.addoption(
        '--file-log-path',
        action='store',
        default="",
        help='Log path for the test',
    )

@pytest.fixture(scope="session")
def instrument_id(request) -> InstrumentId:
    value = request.config.getoption('--instrument-id')
    return InstrumentId.from_str(value)

# @pytest.fixture(scope="session")
# def instrument(event_loop, cache, instrument_provider, instrument_id) -> FuturesContract:
#
#     instrument = event_loop.run_until_complete(
#         instrument_provider.load_async(instrument_id),
#     )
#
#     if instrument is None:
#         for instrument in instrument_provider.list_all():
#             print(instrument)
#         raise RuntimeError(f"Instrument not found: {instrument_id}")
#
#     cache.add_instrument(instrument)
#
#     return instrument


@pytest.fixture(scope="session")
def event_loop():
    # loop = asyncio.get_event_loop_policy().new_event_loop()
    loop = asyncio.get_event_loop()
    # asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def clock() -> LiveClock:
    return LiveClock()


@pytest.fixture(scope="session")
def msgbus(clock):
    return MessageBus(
        TestIdStubs.trader_id(),
        clock,
    )


@pytest.fixture(scope="session")
def cache():
    return TestComponentStubs.cache()


@pytest.fixture(scope="session")
def client(event_loop, msgbus, cache, clock) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
    )

    init_logging(level_stdout=LogLevel.DEBUG)

    return client





@pytest.fixture(scope="session")
@pytest.mark.asyncio
def exec_client(event_loop, msgbus, cache, clock, instrument_id) -> InteractiveBrokersExecClient:
    # provider_params = dict(load_ids=[instrument_id.value], **DEFAULT_PROVIDER_PARAMS)
    exec_engine, exec_client, provider, client  = InteractiveBrokersExecEngineFactory.create(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )

    event_loop.run_until_complete(client.connect())
    yield exec_client

@pytest_asyncio.fixture(scope="session")
async def order_setup(exec_client) -> OrderSetup:
    order_setup = OrderSetup(
        exec_client=exec_client,
        data_client=None,
    )
    # await order_setup.close_all()
    # await asyncio.sleep(1)
    yield order_setup
    # await order_setup.close_all()
    # event_loop.run_until_complete(order_setup.close_all())

# @pytest.fixture(scope="session")
# def socket(event_loop) -> InteractiveBrokersClient:
#     return Socket(
#             loop=event_loop,
#             host="127.0.0.1",
#             port=4002,
#             client_id=1,
#             callback=None,
#     )

# @pytest.fixture()
# def delay() -> OrderSetup:
#     # asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.25))
#     return

# @pytest.fixture(scope="session")
# def log(request, clock, instrument_id) -> None:
#     file_logging = request.config.getoption('--file-logging')
#     file_log_path = request.config.getoption('--file-log-path')





# @pytest.fixture(scope="session")
# def log(logger) -> Logger:
#     return Logger("pytest")


# import asyncio
# from pyfutures.tests.adapters.interactive_brokers.demo.factories import (
#     InteractiveBrokersExecEngineFactory,
# )

# import pytest
# import pytest_asyncio
# from nautilus_trader.config import LiveExecEngineConfig
# from nautilus_trader.live.execution_engine import LiveExecutionEngine
# from nautilus_trader.model.identifiers import AccountId
# from nautilus_trader.model.identifiers import InstrumentId
# from nautilus_trader.model.objects import Price
# from nautilus_trader.test_kit.stubs.events import TestEventStubs
# from nautilus_trader.test_kit.stubs.execution import TestExecStubs

# # fmt: off
# from pyfutures.adapter. import IB_VENUE
# from pyfutures.adapter..config import InteractiveBrokersExecClientConfig
# from pyfutures.adapter..config import InteractiveBrokersInstrumentProviderConfig
# from pyfutures.adapter..execution import InteractiveBrokersExecutionClient
# from pyfutures.adapter..factories import InteractiveBrokersLiveExecClientFactory
# from pyfutures.tests.adapters.order_setup import OrderSetup



# def pytest_addoption(parser):
#     parser.addoption(
#         '--instrument-id',
#         action='store',
#         # default="EOE=MFA=Z23.FTA",
#         default="DC=DA=FUT=2024G.CME",
#         help='Base URL for the API tests',
#     )
#     parser.addoption(
#         '--file-logging',
#         action='store',
#         default=False,
#         help='Enable file logging for the test',
#     )
#     parser.addoption(
#         '--file-log-path',
#         action='store',
#         default="",
#         help='Log path for the test',
#     )

# @pytest.fixture(scope="session")
# def instrument_id(request) -> InstrumentId:
#     value = request.config.getoption('--instrument-id')
#     return InstrumentId.from_str(value)

# @pytest.fixture(scope="session")
# def instrument(event_loop, cache, instrument_provider, instrument_id) -> FuturesContract:
#
#     instrument = event_loop.run_until_complete(
#         instrument_provider.load_async(instrument_id),
#     )
#
#     if instrument is None:
#         for instrument in instrument_provider.list_all():
#             print(instrument)
#         raise RuntimeError(f"Instrument not found: {instrument_id}")
#
#     cache.add_instrument(instrument)
#
#     return instrument



# @pytest.fixture(scope="session")
# def event_loop(request):
#     loop = asyncio.new_event_loop()
#     yield loop
#     loop.close()




# DEFAULT_PROVIDER_PARAMS = dict(
#         chain_filters={
#             'FMEU': lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
#         },
#         parsing_overrides={
#             "MIX": {
#                 "price_precision": 0,
#                 "price_increment": Price(5, 0),
#             },
#         },
#     )


# @pytest.fixture(scope="session")
# @pytest.mark.asyncio
# def exec_client(event_loop, msgbus, cache, clock, instrument_id) -> InteractiveBrokersExecutionClient:
#     print("INSTURMENT ID")
#     print(instrument_id.value)
#     provider_params = dict(load_ids=[instrument_id.value], **DEFAULT_PROVIDER_PARAMS)
#     exec_engine, exec_client, provider, client  = InteractiveBrokersExecEngineFactory.create(
#         loop=event_loop, 
#         msgbus=msgbus, 
#         cache=cache, 
#         clock=clock,
#         provider_config=InteractiveBrokersInstrumentProviderConfig(**provider_params)
#     )

#     event_loop.run_until_complete(client.connect())
#     yield exec_client





# @pytest_asyncio.fixture(scope="session")
# async def order_setup(exec_client) -> OrderSetup:
#     print("ORDER SETUP")
#     order_setup = OrderSetup(
#         exec_client=exec_client,
#         data_client=None,
#     )
    # await order_setup.close_all()
    # await asyncio.sleep(1)
    # yield order_setup
    # await order_setup.close_all()
    # event_loop.run_until_complete(order_setup.close_all())

# @pytest.fixture(scope="session")
# def socket(event_loop) -> InteractiveBrokersClient:
#     return Socket(
#             loop=event_loop,
#             host="127.0.0.1",
#             port=4002,
#             client_id=1,
#             callback=None,
#     )

# @pytest.fixture()
# def delay() -> OrderSetup:
#     # asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.25))
#     return

# @pytest.fixture(scope="session")
# def log(request, clock, instrument_id) -> None:
#     file_logging = request.config.getoption('--file-logging')
#     file_log_path = request.config.getoption('--file-log-path')





# @pytest.fixture(scope="session")
# def log(logger) -> Logger:
#     return Logger("pytest")
