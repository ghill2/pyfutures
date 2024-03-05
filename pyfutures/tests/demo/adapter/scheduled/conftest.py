import asyncio
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytest

from pyfutures.adapter.client.client import InteractiveBrokersClient


def future_for_timestamp(timestamp):
    """
    Creates a future that resolves to True when the current time reaches the given timestamp.

    Args:
    ----
        timestamp: A timestamp in milliseconds since the epoch or a datetime object.

    Returns:
    -------
        A concurrent.futures.Future object that resolves to True when the timestamp is reached.
    """
    target_time = datetime.fromtimestamp(timestamp / 1000, timezone.utc)  # Convert ms to datetime

    def is_future_reached():
        return datetime.now(timezone.utc) >= target_time

    from concurrent.futures import Future

    future = Future()
    future.set_result(is_future_reached())
    return future


async def dummy_timestamps():
    """
    generate a list of timestamps in the future
    used for manual testing
    """
    now = pd.Timestamp.now()


# @pytest.fixture()
# def rows():
#     rows = IBTestProviderStubs.universe_rows()
#     return rows[0:2]
#
@pytest.fixture(scope="session")
def shared_queue():
    queue = [1, 2, 3, 4]  # Example list of integers
    yield queue

    # Optionally clear the queue after all tests (if needed):
    del queue[:]


async def test_function():
    """This function replaces a client test function that runs a test on a single instrument"""
    print("test with id {} is running")
    asyncio.sleep(1)


# @pytest.fixture(scope="session")
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
    return client
