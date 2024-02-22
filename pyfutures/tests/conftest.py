# TODO: temporary location, where to keep this conftest so test_stats.py and the IB tests have access to the fixtures?
import asyncio

import pytest
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient


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
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        host="127.0.0.1",
        port=4002,
        client_id=1,
    )

    init_logging(level_stdout=LogLevel.DEBUG)

    return client
