import asyncio
import logging

import pytest


@pytest.mark.asyncio()
async def test_connect(event_loop, mode, client):
    # client = await BytestringClientStubs(mode=mode, loop=event_loop).client(
    #     loop=event_loop
    # )
    await client.connect()
    await client.request_account_summary()
