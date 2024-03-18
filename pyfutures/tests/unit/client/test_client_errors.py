import asyncio
from unittest.mock import Mock
from unittest.mock import AsyncMock

import pytest

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.tests.unit.client.mock_server import MockServer
from pyfutures.tests.unit.client.stubs import ClientStubs
import functools


# PROBLEMS:
# no exceptions are displayed in the _listen_task
# any exceptions encountered will cancel the _listen_task, making the client unresponsive
# any exceptions encountered will not send back an exception to the request object
# the request happens, then exception is encountered in the callback
# we can log a message, but the client is still waiting for a timeout, because the request is still awaiting
#
# possibly expand this test to cover all methods on the client
#
#
class MockStreamReader(asyncio.StreamReader):
    def __init__(self, loop=None):
        super().__init__(loop=loop)
        self._message_queue = asyncio.Queue()

    async def readline(self, size=-1):
        print("READLINE")
        if self._message_queue.empty():
            return await super().readline(size)
        else:
            return await self._message_queue.get()

    async def readuntil(self, separator, size=-1):
        print("READUNTIL")
        while self._message_queue.empty():  # Wait until data is available
            await asyncio.sleep(0)  # Yield control to other tasks
            return await self._message_queue.get()

    async def read(self, n=-1):
        print("READ")
        asyncio.sleep(50)

    def put_message(self, message):
        self._message_queue.put_nowait(message)


class MockStreamWriter(asyncio.StreamWriter):
    def __init__(self, transport, protocol, reader=None, loop=None):
        super().__init__(transport, protocol, reader, loop)
        # self._message_queue = reader._message_queue
        self._reader = reader

    def write(self, data):
        print("WRITE CALLED")
        data = b"\x00\x00\x00*176\x0020240308 13:30:34 Greenwich Mean Time\x00"

        # schedule read bytes after this current execution path
        asyncio.create_task(self._reader._message_queue.put_nowait(data))

    async def drain(self):
        # No actual writing needed, just wait for the queue to drain
        # while not self._message_queue.empty():
        # await asyncio.sleep(0)
        pass

    # def get_messages(self):
    #     messages = []
    #     while not self._message_queue.empty():
    #         messages.append(self._message_queue.get_nowait())
    #     return messages
    #


@pytest.mark.asyncio()
async def test_mock_server(mocker, event_loop):
    print(event_loop)
    client = ClientStubs.client(loop=event_loop, request_timeout_seconds=10)

    async def mock_create_connection(*args, **kwargs):
        """
        The testing version of asyncio.open_connection() - asyncio/streams.py
        """
        print("EVENT_LOOP")
        print(event_loop)
        reader = MockStreamReader(loop=event_loop)
        writer = MockStreamWriter(None, None, reader, loop=event_loop)
        return reader, writer

    # client._connection.create_connection = AsyncMock(side_effect=lambda *args, **kwargs: mock_create_connection(event_loop))
    client._connection.create_connection = AsyncMock(side_effect=mock_create_connection)
    # mocker.patch.object(client._connection, "create_connection", side_effect=mock_create_connection)
    await client.connect()


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
