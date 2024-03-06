import asyncio

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.model.identifiers import TraderId

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.tests.test_kit import IBTestProviderStubs


# RUN THESE TESTS WITH:
# -n auto -> use pytest-xdist and auto decide amount of tests to run based on the amount of CPU cores
# test_universe_scheduled.py --capture=sys --tui -n auto
# @pytest.mark.parametrize("timestamp", dummy_timestamps())
#
@pytest.mark.parametrize("row", IBTestProviderStubs.universe_rows())
@pytest.mark.asyncio()
async def test_scheduled(shared_queue, row):
    """
    - a client_id is popped from the queue to use for the duration of the test
    - when the test finishes, the client_id is returned into the pool
    """
    init_logging(level_stdout=LogLevel.DEBUG)

    print("THIS TEST RUN, was it captured?")

    # fut = future_for_timestamp()
    client_id = shared_queue.pop()

    clock = LiveClock()
    client = InteractiveBrokersClient(
        loop=asyncio.get_event_loop(),
        msgbus=MessageBus(
            TraderId("TESTER-000"),
            clock,
        ),
        cache=Cache(database=None),
        clock=clock,
        host="127.0.0.1",
        port=4002,
        client_id=client_id,
    )

    await client.connect()
    results = await client.request_contract_details(row.contract_cont)
    print(results)
    shared_queue.push(client_id)

    # async def test_scheduled_single_process(client):
