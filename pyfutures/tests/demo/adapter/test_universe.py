from pyfutures.client.client import InteractiveBrokersClient
import pytest
import asyncio
from pyfutures.tests.test_kit import IBTestProviderStubs

from pyfutures.tests.demo.client.stubs import ClientStubs


from pyfutures.logger import LoggerAttributes
import random
import logging


async def run_async(loop, client_id):
    client = InteractiveBrokersClient(
        loop=loop,
        client_id=client_id,
        host="127.0.0.1",
        port=4002,
        api_log_level=logging.DEBUG,
    )
    # await asyncio.sleep(random.randrange(2, 10))
    await client.connect()
    while True:
        # await asyncio.sleep(random.randrange(5, 10, step=1))
        await asyncio.sleep(10)
        print(await client.request_accounts())


@pytest.mark.asyncio()
async def test_multiple_client_id(event_loop):
    tasks = []
    for i in range(1, 150):
        print("=========================================")
        # tasks.append(run_async(loop=event_loop, client_id=i))
        # client = ClientStubs.uncached_client(client_id=i, loop=event_loop)
        event_loop.create_task(run_async(event_loop, i))
        await asyncio.sleep(3)
    # await asyncio.gather(*tasks)
