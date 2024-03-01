import asyncio
import logging
import pytest
import sys

from pyfutures.adapters.interactive_brokers.client.client import (
    InteractiveBrokersClient,
)
from pyfutures.adapters.interactive_brokers.client.connection import Connection

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def client(event_loop) -> InteractiveBrokersClient:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
        
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

