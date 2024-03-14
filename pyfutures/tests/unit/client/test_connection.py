import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from pyfutures.client.connection import Connection
from pyfutures.tests.unit.client.mock_server import MockServer
from pyfutures.tests.unit.client.stubs import ClientStubs


pytestmark = pytest.mark.unit


class TestConnection:
    def setup_method(self):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        self.connection: Connection = ClientStubs.connection(client_id=1)
        self.mock_server = MockServer()

    @pytest.mark.asyncio()
    async def test_connect(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        # Arrange
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()

        mocker.patch(
            "asyncio.open_connection",
            return_value=(mock_reader, mock_writer),
        )

        # Act
        await self.connection._connect()

        # Assert
        asyncio.open_connection.assert_called_once_with(
            self.connection.host,
            self.connection.port,
        )
        assert self.connection._reader == mock_reader
        assert self.connection._writer == mock_writer

        listen_started = not self.connection._listen_task.done() and not self.connection._listen_task.cancelled()
        assert listen_started
        assert self.connection._listen_task in asyncio.all_tasks(self.connection.loop)

    @pytest.mark.skip(reason="hangs when running entire test suite")
    @pytest.mark.asyncio()
    async def test_handshake_client_id_1(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        mocker.patch(
            "asyncio.open_connection",
            return_value=(self.mock_server.reader, self.mock_server.writer),
        )

        # await asyncio.wait_for(self._is_connected.wait(), timeout_seconds)
        await self.connection.connect(timeout_seconds=1)
        assert self.connection.is_connected

    @pytest.mark.skip(reason="hangs when running entire test suite")
    @pytest.mark.asyncio()
    async def test_handshake_client_id_2(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        connection: Connection = ClientStubs.connection(client_id=2)

        # Act
        mocker.patch(
            "asyncio.open_connection",
            return_value=(self.mock_server.reader, self.mock_server.writer),
        )

        await self.connection.connect(timeout_seconds=1)

        assert connection.is_connected

    @pytest.mark.asyncio()
    async def test_handshake_client_id_10(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        connection: Connection = ClientStubs.connection(client_id=10)

        # Act
        mocker.patch(
            "asyncio.open_connection",
            return_value=(self.mock_server.reader, self.mock_server.writer),
        )

        await self.connection.connect(timeout_seconds=1)

        assert connection.is_connected

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_empty_byte_handles_disconnect(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        self.connection._handle_disconnect = AsyncMock()
        mock_reader = AsyncMock()
        mock_reader.read.return_value = b""

        mocker.patch(
            "asyncio.open_connection",
            return_value=(mock_reader, None),
        )

        await self.connection._connect()  # start listen task
        await asyncio.sleep(0)
        self.connection._handle_disconnect.assert_called_once()

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_connection_reset_error_handles_disconnect(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        self.connection._handle_disconnect = AsyncMock()

        def raise_error(*args, **kwargs):
            raise ConnectionResetError

        self.mock_server.reader.read.side_effect = raise_error

        mocker.patch(
            "asyncio.open_connection",
            return_value=(AsyncMock(), None),
        )

        await self.connection._connect()  # start listen task
        await asyncio.sleep(0)
        self.connection._handle_disconnect.assert_called_once()

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_disconnect_resets(self, mocker):
        # NOTE: mocker fixture required to avoid hanging

        mocker.patch(
            "asyncio.open_connection",
            return_value=(self.mock_server.reader, self.mock_server.writer),
        )

        await self.connection.connect()
        assert self.connection.is_connected
        await self.connection._handle_disconnect()

        assert not self.connection._is_connected.is_set()
        assert len(self.connection._handshake_message_ids) == 0
        assert self.connection._reader is None
        assert self.connection._writer is None
        assert self.connection._listen_task is None
        assert not self.connection.is_connected
        assert self.connection._monitor_task is not None

    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_connect_after_disconnect(self, mocker):
        mocker.patch(
            "asyncio.open_connection",
            return_value=(self.mock_server.reader, self.mock_server.writer),
        )

        await self.connection.connect()
        assert self.connection.is_connected

        self.mock_server.disconnect()
        await asyncio.sleep(1)
        assert not self.connection.is_connected

    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_connect_resets_on_connection_refused_error(self, connection, mock_socket):
        pass

    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_reset_closes_writer(self, connection):
        pass


# # Arrange
# handshake_responses = [
#     [b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'],
#     [b'15\x001\x00DU1234567\x00', b'9\x001\x006\x00'],
# ]

# def send_mocked_response(_):
#     responses = handshake_responses.pop(0)
#     while len(responses) > 0:
#         connection._handle_msg(responses.pop(0))

# connection._sendMsg = Mock(side_effect=send_mocked_response)
# from ibapi import comm
# buf = b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'
# buf = b"\x00\x00\x00\x089\x001\x00530\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00\x00\x00\x0044\x002\x00-1\x002106\x00HMDS data farm connection is OK:ushmds\x00\x00"
# (size, msg, buf) = comm.read_msg(buf)

# Assert
# sent_messages: list[bytes] = [x[0][0] for x in connection._sendMsg.call_args_list]

# sent_expected = [
#     b"API\x00\x00\x00\x00\nv176..176 ",
#     b"\x00\x00\x00\x0871\x002\x001\x00\x00",
# ]
# assert sent_messages == sent_expected


# client.wait_until_connected = MagicMock(return_value)
# asyncio.create_task(
# mock_socket.connect(client._conn)
# )

# mocked_data = [b'4\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00']

# Simulate receiving data from the mocked reader
# mocked_response = b"Mocked response"
# mock_reader.read.return_value = mocked_response
# received_data = await connection._listen()
# print(received_data)
#
# handshake:
# TODO: there is no error displayed if you send a request when the client is not connected
# change handshake to wait for
# do demo tests afterwards
# remove farm is ok
# make another branch to do these tests
# make sure all exceptions are shown

# make sure exceptions are shwon in the logs for the errors handled
# redo subscriptions on reconnect
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
from pyfutures.logger import LoggerAttributes

LoggerAttributes.level = logging.DEBUG


# NOTE: mocker fixture required to avoid hanging


# as there is no clean way to wait for an asyncio.Event to be unset
# set timeout for the test instead and use While
@pytest.mark.timeout(0.5)
@pytest.mark.asyncio()
async def test_reconnection_on_empty_byte(mocker):
    connection = ClientStubs.connection(client_id=1)
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
    connection = ClientStubs.connection(client_id=1)
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
