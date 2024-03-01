import pytest
from pyfutures.adapters.interactive_brokers.client.connection import Connection
import logging


@pytest.fixture(scope="session")
def connection(event_loop) -> Connection:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return Connection(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
    )
