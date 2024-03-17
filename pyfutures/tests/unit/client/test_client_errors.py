import asyncio
from unittest.mock import Mock

import pytest

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.tests.unit.client.mock_server import MockServer
from pyfutures.tests.unit.client.stubs import ClientStubs


# PROBLEMS:
# no exceptions are displayed in the _listen_task
# any exceptions encountered will cancel the _listen_task, making the client unresponsive
# any exceptions encountered will not send back an exception to the request object
# the request happens, then exception is encountered in the callback
# we can log a message, but the client is still waiting for a timeout, because the request is still awaiting
#
# possibly expand this test to cover all methods on the client
#


@pytest.mark.asyncio()
async def test_handle_exception_in_callback(mocker, event_loop):
    """
    if any of the code on the response side encounters an exception the client will still keep running
    if an exception is encountered in the callbacks
      - the client should display the exception in the logs
      - the client should send back a ClientException in the reqId ?
        -- we decided to not implemented this, the caller / request can wait until asyncio.Timeout
      - the _listen_task should not cancel
    """
    client = ClientStubs.client(loop=event_loop, request_timeout_seconds=10)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )
    await client.connect(timeout_seconds=0.1)
    assert client._connection.is_connected

    # this will receive a TimeoutError as the callback raised an exception
    client.managedAccounts = Mock(side_effect=lambda accountsList: Exception)

    mock_server.queue_response(req=b"\x00\x00\x00\x0517\x001\x00", res=b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00")

    try:
        # async with mock_server.responses(accounts_res):
        print("SENDING REQUEST_ACCOUNTS 1ST TIME")
        await client.request_accounts()
    except asyncio.TimeoutError:
        pass

    mock_server.queue_response(req=b"\x00\x00\x00\x0517\x001\x00", res=b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00")

    def mock_managed_accounts(self, *args, **kwargs):
        return InteractiveBrokersClient.managedAccounts(client, accountsList="DU1234567,")

    client.managedAccounts = Mock(side_effect=mock_managed_accounts)

    # async with mock_server.responses(accounts_res):
    #
    print("SENDING REQUEST_ACCOUNTS 2ND TIME")

    # with mocker.patch.object(InteractiveBrokersClient, "managedAccounts", mock_managed_accounts):
    accounts = await client.request_accounts()
    await asyncio.sleep(10)
    # print(accounts)
    # captured = capsys.readouterr()
    # print(captured)
    #
    #    #
    # # assert self.connection._listen_task in
    # print(asyncio.all_tasks(event_loop))
    #
    # # returns False if the task is still running
    # print(client._connection._listen_task.done())
    #
    #
    # the reader wait has to be dependant on


@pytest.mark.asyncio()
async def test_handle_exception_in_callback_2(mocker, event_loop):
    client = ClientStubs.client(loop=event_loop, request_timeout_seconds=10)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )
    await client.connect(timeout_seconds=0.1)
    assert client._connection.is_connected

    mock_server.queue_response(req=b"\x00\x00\x00\x0517\x001\x00", res=b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00")

    def mock_managed_accounts(self, *args, **kwargs):
        return InteractiveBrokersClient.managedAccounts(client, accountsList="DU1234567,")

    client.managedAccounts = Mock(side_effect=mock_managed_accounts)

    # async with mock_server.responses(accounts_res):
    #
    print("SENDING REQUEST_ACCOUNTS 2ND TIME")

    # with mocker.patch.object(InteractiveBrokersClient, "managedAccounts", mock_managed_accounts):
    accounts = await client.request_accounts()
    await asyncio.sleep(10)
