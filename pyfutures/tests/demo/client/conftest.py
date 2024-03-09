import asyncio
import logging

import pytest

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.connection import Connection


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def client(event_loop) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
        log_level=logging.DEBUG,
        api_log_level=logging.INFO,
    )
    return client


@pytest.fixture(scope="session")
def connection(event_loop) -> Connection:
    return Connection(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
    )
