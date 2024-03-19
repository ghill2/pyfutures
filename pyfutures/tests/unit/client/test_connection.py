import asyncio
import logging
from unittest.mock import AsyncMock
from unittest.mock import Mock

from unittest.mock import MagicMock

import pytest

from pyfutures.client.connection import Connection
from pyfutures.client.objects import ClientSubscription
from pyfutures.logger import LoggerAttributes

# from pyfutures.tests.unit.client.mock_server import MockServer
from pyfutures.tests.unit.client.stubs import UnitClientStubs
import async_solipsism

from async_solipsism.socket import Socket
from async_solipsism.socket import Queue


# pytestmark = pytest.mark.unit


# handshake:
# TODO: there is no error displayed if you send a request when the client is not connected
# make sure all exceptions are shown

# ----
# to test auto restart not asking for credentials:
# configure settings of gateway to quit within N minutes of the current time
#
# the writer might have to queue the responses
# first test with demo
# then once connection is working, test with mock server
#

# not using handle_disconnect()
# the terminology is confusing.
# this is supposed to handle a reconnect after disconnection
# not a graceful shut down or disconnect
#

LoggerAttributes.level = logging.DEBUG


class MockSocket:
    def __init__(self):
        self._queue = asyncio.Queue()  # Queue to store data
        self.respond = asyncio.Event()
        self.should_wait = False

    def write(self, msg: bytes):
        """
        For the handshake, responses cannot be queued using this function
        as this is called from inside the connect() function,
        the responses can only be added to the queue after the entire connect() method has run
        they have to be queued before
        """
        # Simulate writing data (you might implement additional logic here)
        print(f"MockSocket: Writing data: {msg}")

    async def drain(self):
        pass

    async def read(self, _):
        while True:
            while not self._queue.empty():
                data = await self._queue.get()
                return data
            # await asyncio.sleep(0)

    async def put(self, messages: list | bytes):
        # Add data to the queue
        if isinstance(messages, bytes):
            messages = [messages]

        for msg in messages:
            await self._queue.put(msg)

    # async def set_wait_mode(self):
    #     """
    #         should_block ->
    #     """
    #


@pytest.mark.asyncio()
async def test_connect_2(mocker):
    client = UnitClientStubs.client(request_timeout_seconds=20)

    UNIT = False

    if UNIT:
        socket = MockSocket()
        client.conn.open_connection = AsyncMock(return_value=(socket, socket))
    else:
        socket = AsyncMock()

    ## when sending a client request, return the future immediately so it can be handled in the test function
    # def mock_wait_for_request(request):
    #     print(request)
    #     return request
    #
    # client._wait_for_request - AsyncMock(side_effect=mock_wait_for_request)
    # mock the reader and writer with a test implementation
    #
    await socket.put(b"\x00\x00\x00*176\x0020240308 13:30:34 Greenwich Mean Time\x00")
    await socket.put(b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00")

    await client.connect()
    print("after connect")
    # await asyncio.sleep(0)

    summary = await client.request_account_summary()
    print(summary)

    # await asyncio.sleep(20)


@pytest.mark.asyncio()
async def test_connect():
    client = UnitClientStubs.client(request_timeout_seconds=20)
    # client.conn._wait_for_request - AsyncMock(side_effect=lambda request: request)
    # client.conn._connect_start_tasks - AsyncMock()
    await client.connect()
    fut = await client.request_account_summary()

    buf = b""
    fields = await client.conn.read(buf=buf)
    print(fields)
    buf = b""
    fields = await client.conn.read(buf=buf)
    print(fields)
    buf = b""
    fields = await client.conn.read(buf=buf)
    print(fields)
    buf = b""
    fields = await client.conn.read(buf=buf)
    print(fields)


# NOTE: mocker fixture required to avoid hanging


# as there is no clean way to wait for an asyncio.Event to be unset
# set timeout for the test instead and use While
@pytest.mark.timeout(0.5)
@pytest.mark.asyncio()
async def test_reconnection_on_empty_byte(mocker):
    """
    When the client receives an empty byte, the client should attempt reconnect
    also tests for reconnecting subscriptions
    """
    connection = UnitClientStubs.connection(client_id=1)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )

    await connection.connect(timeout_seconds=0.1)

    assert connection.is_connected

    mock_server.disconnect()

    # _connect_monitor_task() is asyncio.sleeping at this time
    # wait until the next iteration loop that checks connection.is_connected
    while connection.is_connected:
        await asyncio.sleep(0.01)

    # now wait for reconnect
    await connection._is_connected.wait()


@pytest.mark.timeout(0.5)
@pytest.mark.asyncio()
async def test_reconnection_on_conn_reset_error(mocker):
    connection = UnitClientStubs.connection(client_id=1)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )

    await connection.connect(timeout_seconds=0.1)

    assert connection.is_connected

    # await mock_server.disconnect()
    mock_server.send_response(ConnectionResetError)

    # _connect_monitor_task() is asyncio.sleeping at this time
    # wait until the next iteration loop that checks connection.is_connected
    while connection.is_connected:
        await asyncio.sleep(0.01)

    # now wait for reconnect
    await connection._is_connected.wait()


@pytest.mark.timeout(0.5)
@pytest.mark.asyncio()
async def test_reconnect_subscriptions(mocker):
    """
    When the client reconnects, the existing subscriptions should resubscribe
    """
    # subscription = self.client.subscribe_bars(
    #     contract=Contract(),
    #     what_to_show=WhatToShow.BID,
    #     bar_size=BarSize._1_MINUTE,
    #     callback=Mock(),
    # )
    subscribe_mock = Mock()
    subscriptions = {-10: ClientSubscription(id=10, subscribe=subscribe_mock, cancel=Mock(), callback=Mock())}
    connection = ClientStubs.connection(client_id=1, subscriptions=subscriptions)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )

    await connection.connect(timeout_seconds=0.1)

    assert connection.is_connected

    mock_server.disconnect()

    # _connect_monitor_task() is asyncio.sleeping at this time
    # wait until the next iteration loop that checks connection.is_connected
    while connection.is_connected:
        await asyncio.sleep(0.01)

    # now wait for reconnect
    await connection._is_connected.wait()

    assert subscribe_mock.call_count == 1


# TODO: cant really test this with unit tests.
#
# @pytest.mark.skip(reason="hangs when running entire test suite")
#
# @pytest.mark.asyncio()
# async def test_handshake_client_id_1(mocker, event_loop):
#     connection = ClientStubs.connection(client_id=1)
#     mock_server = MockServer()
#     mocker.patch(
#         "asyncio.open_connection",
#         return_value=(mock_server.reader, mock_server.writer),
#     )
#     await connection.connect(timeout_seconds=1)
#
# @pytest.mark.skip(reason="hangs when running entire test suite")
# @pytest.mark.asyncio()
# async def test_handshake_client_id_2(mocker):
#     # NOTE: mocker fixture required to avoid hanging
#
#     connection: Connection = ClientStubs.connection(client_id=2)
#
#     # Act
#     mocker.patch(
#         "asyncio.open_connection",
#         return_value=(self.mock_server.reader, self.mock_server.writer),
#     )
#
#     await self.connection.connect(timeout_seconds=1)
#
#     assert connection.is_connected
#


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_reset_closes_writer(self, connection):
    pass


# class TestConnection:
# def setup_method(self):
#     logger = logging.getLogger()
#     logger.setLevel(logging.DEBUG)
#
#     self.connection: Connection = ClientStubs.connection(client_id=1)
#     self.mock_server = MockServer()
# @pytest.mark.skip()
# @pytest.mark.asyncio()
# async def test_reconnection_on_conn_reset_error_old(mocker):
#     """
#     _listen_task ConnectionResetError should call _handle_disconnect()
#     when a connection reset error is raised, the client should attempt to reconnect
#     test: raise the error, then wait until self.is_connected is True
#     """
#     # NOTE: mocker fixture required to avoid hanging
#
#     self.connection._handle_disconnect = AsyncMock()
#
#     def raise_error(*args, **kwargs):
#         raise ConnectionResetError
#
#     self.mock_server.reader.read.side_effect = raise_error
#
#     mocker.patch(
#         "asyncio.open_connection",
#         return_value=(AsyncMock(), None),
#     )
#
#     await self.connection._connect()  # start listen task
#     await asyncio.sleep(0)
#     self.connection._handle_disconnect.assert_called_once()
# @pytest.mark.skip()
# @pytest.mark.asyncio()
# async def test_reconnection_on_empty_byte_old(mocker):
#     """
#     when an empty byte is received on the socket, the client should attempt to reconnect
#     """
#     # NOTE: mocker fixture required to avoid hanging
#     connection: Connection = ClientStubs.connection(client_id=1)
#
#     connection._handle_disconnect = AsyncMock()
#     mock_reader = AsyncMock()
#     mock_reader.read.return_value = b""
#
#     mocker.patch(
#         "asyncio.open_connection",
#         return_value=(mock_reader, None),
#     )
#
#     await connection._connect()  # start listen task
#     await asyncio.sleep(0)
#     connection._handle_disconnect.assert_called_once()
#
# @pytest.mark.asyncio()
# async def test_connect(self, mocker):
#     # NOTE: mocker fixture required to avoid hanging
#
#     # Arrange
#     mock_reader = AsyncMock()
#     mock_writer = AsyncMock()
#
#     mocker.patch(
#         "asyncio.open_connection",
#         return_value=(mock_reader, mock_writer),
#     )
#
#     # Act
#     await self.connection._connect()
#
#     # Assert
#     asyncio.open_connection.assert_called_once_with(
#         self.connection.host,
#         self.connection.port,
#     )
#     assert self.connection._reader == mock_reader
#     assert self.connection._writer == mock_writer
#
#     listen_started = not self.connection._listen_task.done() and not self.connection._listen_task.cancelled()
#     assert listen_started
#     assert self.connection._listen_task in asyncio.all_tasks(self.connection.loop)
