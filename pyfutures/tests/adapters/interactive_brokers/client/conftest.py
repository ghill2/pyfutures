import pytest
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.client.connection import Connection

@pytest.fixture
def client(loop) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
        loop=loop,
        host="127.0.0.1",
        port=4002,
        client_id=1,
    )
    return client

@pytest.fixture
def connection(loop) -> Connection:
    return Connection(
            loop=loop,
            handler=lambda x: print(x),
            host="127.0.0.1",
            port=4002,
        )