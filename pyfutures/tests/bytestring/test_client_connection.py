import asyncio
import logging

import pytest


@pytest.mark.asyncio()
async def test_connect(client):
    # client = await BytestringClientStubs(mode=mode, loop=event_loop).client(
    #     loop=event_loop
    # )
    await client.connect()
    await client.request_account_summary()


@pytest.mark.asyncio()
async def test_reconnect(event_loop, mode, client):
    """
    The bytestrings for this test were created by exiting and reopening gateway manually

    There are ways to automate this test:

    """
    # client = await BytestringClientStubs(mode=mode, loop=event_loop).client(
    #     loop=event_loop
    # )
    await client.connect()
    await client.request_account_summary()
