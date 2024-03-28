import asyncio

import pytest


@pytest.mark.asyncio()
async def test_reconnect(client):
    """
    The bytestrings for this test were created by exiting and reopening gateway manually

    There are ways to automate this test:
    - use IB gateway module
    - use mock_server_subproc with the demo test
       -- instruct mock_server to feed_eof
       -- this will require mocking _wait_for_request to return immediately

    """
    await client.connect()
    await client.request_account_summary()

    print("Now close and open Gateway manually...")

    # wait until the client is disconnected
    while client.conn.is_connected.is_set():
        await asyncio.sleep(0.5)

    # wait until client reconnects automatically
    while not client.conn.is_connected.is_set():
        await asyncio.sleep(1)
        print("Waiting for gateway to reconnect automatically")

    await client.request_account_summary()
