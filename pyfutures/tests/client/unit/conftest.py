import asyncio
import logging
import pytest
import sys

from pyfutures.adapters.interactive_brokers.client.client import (
    InteractiveBrokersClient,
)

from pyfutures.tests.client.unit.mock_socket import MockSocket

from pyfutures.adapters.interactive_brokers.client.connection import Connection

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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


@pytest.fixture
def mock_socket() -> MockSocket:
    return MockSocket()
