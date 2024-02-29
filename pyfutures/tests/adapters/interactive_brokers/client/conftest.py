import pytest
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient

@pytest.fixture(scope="session")
def client(event_loop, msgbus, cache, clock) -> InteractiveBrokersClient:
    client = InteractiveBrokersClient(
        loop=event_loop,
        host="127.0.0.1",
        port=4002,
        client_id=1,
    )
    return client
