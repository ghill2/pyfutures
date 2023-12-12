import asyncio

import pytest
import os
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Price
from nautilus_trader.common.component import MessageBus
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.adapters.interactive_brokers.client.connection import Connection

# fmt: off
from pyfutures.adapters.interactive_brokers import IB_VENUE
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapters.interactive_brokers.execution import InteractiveBrokersExecutionClient
from pyfutures.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider
from pyfutures.tests.adapters.order_setup import OrderSetup
from pyfutures.adapters.interactive_brokers.client.socket import Socket

def pytest_addoption(parser):
    parser.addoption(
        '--instrument-id',
        action='store',
        default="EOE-MFA-Z23.FTA",
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
def logger(request, clock, instrument_id) -> Logger:
    file_logging = request.config.getoption('--file-logging')
    file_log_path = request.config.getoption('--file-log-path')
    return Logger(
        clock,
        level_stdout=LogLevel.DEBUG,
        file_logging=bool(file_logging),
        file_name=file_log_path,
    )

@pytest.fixture(scope="session")
def log(logger) -> LoggerAdapter:
    return LoggerAdapter("pytest", logger)

@pytest.fixture(scope="session")
def msgbus(clock, logger):
    return MessageBus(
        TestIdStubs.trader_id(),
        clock,
        logger,
    )

@pytest.fixture(scope="session")
def cache(logger):
    return TestComponentStubs.cache(logger)

@pytest.fixture(scope="session")
def socket(event_loop, logger) -> InteractiveBrokersClient:
    return Socket(
            loop=event_loop,
            logger=logger,
            host="127.0.0.1",
            port=4002,
            client_id=1,
            callback=None,
    )
    
@pytest.fixture(scope="session")
def client(event_loop, msgbus, cache, clock, logger) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
            loop=event_loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            host="127.0.0.1",
            port=4002,
            client_id=1,
    )
    return client

@pytest.fixture(scope="session")
def instrument_provider(client, logger) -> InteractiveBrokersInstrumentProvider:

    config = InteractiveBrokersInstrumentProviderConfig(
        chain_filters={
            'FMEU': lambda x: not x.contract.localSymbol.endswith("D"),
        },
        parsing_overrides={
            "MIX": {
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        },
    )

    instrument_provider = InteractiveBrokersInstrumentProvider(
        client=client,
        logger=logger,
        config=config,

    )

    return instrument_provider

@pytest.fixture(scope="session")
def instrument(event_loop, cache, instrument_provider, instrument_id) -> FuturesContract:

    instrument = event_loop.run_until_complete(
        instrument_provider.load_async(instrument_id),
    )

    if instrument is None:
        for instrument in instrument_provider.list_all():
            print(instrument)
        raise RuntimeError(f"Instrument not found: {instrument_id}")

    cache.add_instrument(instrument)

    return instrument


@pytest.fixture(scope="session")
def exec_client(event_loop, msgbus, cache, clock, logger, client, instrument_provider) -> InteractiveBrokersExecutionClient:

    return InteractiveBrokersExecutionClient(
            loop=event_loop,
            client=client,
            account_id=AccountId(f"InteractiveBrokers-{IB_ACCOUNT_ID}"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            instrument_provider=instrument_provider,
            ibg_client_id=1,
    )


@pytest.fixture(scope="session")
def exec_engine(event_loop, exec_client, msgbus, cache, clock, logger, instrument_provider) -> LiveExecutionEngine:

    exec_engine = LiveExecutionEngine(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
        config=LiveExecEngineConfig(
            reconciliation=True,
            inflight_check_interval_ms=0,
            debug=True,
        ),

    )
    # exec_client._set_account_id(exec_client.account_id)
    exec_engine.register_client(exec_client)
    exec_engine.register_default_client(exec_client)

    exec_engine.start()

    account_id = AccountId(f"InteractiveBrokers-{IB_ACCOUNT_ID}")

    cache.add_account(TestExecStubs.margin_account(account_id))

    portfolio = Portfolio(
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
    )

    portfolio.set_specific_venue(IB_VENUE)
    portfolio.update_account(TestEventStubs.margin_account_state())

    return exec_engine

@pytest.fixture()
def delay() -> OrderSetup:
    # asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.25))
    return

@pytest.fixture(scope="session")
def order_setup(event_loop, exec_client, exec_engine) -> OrderSetup:
    order_setup = OrderSetup(
        exec_client=exec_client,
        data_client=None,
    )
    event_loop.run_until_complete(order_setup.close_all())
    event_loop.run_until_complete(asyncio.sleep(1))
    yield order_setup
    event_loop.run_until_complete(order_setup.close_all())
