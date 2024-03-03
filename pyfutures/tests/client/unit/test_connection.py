import asyncio

import pytest

from pyfutures.adapters.interactive_brokers.client.connection import Connection

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

from collections import deque

import logging


class MockSocket:
    """ """

    def __init__(self):
        # append to this queue to receive msg at self._reader.read()
        self._mocked_responses = deque()

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = self._read_generator

        self.respond = asyncio.Event()

        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_writer.write.side_effect = self._write_generator

        # internal storage for req res
        self._requests = deque()
        self._responses = deque()

        self.mock_reader = mock_reader
        self.mock_writer = mock_writer

        self._log = logging.getLogger("MockSocket")

    async def _read_generator(self, _):
        """
        do not yield, otherwise the function is not executed
        """
        self._log.debug("_read_generator")
        if len(self._mocked_responses) == 0:
            self.respond.clear()

        await self.respond.wait()

        response = self._mocked_responses.popleft()
        self._log.debug(f"_read_generator: sending {response}")
        return response

    def _write_generator(self, msg):
        """
        Executed every time Connection._writer.write() is executed
        Ensures a transaction Client -> Gateway, Gateway -> Server
        do not yield, otherwise the function is not executed
        """
        self._log.debug(f"_write_generator: {msg}")
        assert (
            msg == self._requests.popleft()
        ), "expected write value != actual write value"
        responses = self._responses.popleft()
        self._mocked_responses.extend(responses)
        self.respond.set()

    def send_responses(self, responses):
        """
        send a response from gateway to client immediately without an associated client request
        eg, for simulating empty bytestrings
        """
        assert isinstance(responses, list)
        self._mocked_responses.extend(responses)

    def queue_transaction(self, request, responses):
        """

        Queue a response that will be received at a later time
        Example:
          - For the request, send the responses when the client executed _listen socket.read()

        """
        assert isinstance(request, bytes)
        assert isinstance(responses, list)
        self._requests.append(request)
        self._responses.append(responses)

    def queue_handshake(self):
        """
        High Level, Queues the handshake routine
        """
        # handshake message
        self.queue_transaction(
            request=b"API\x00\x00\x00\x00\nv176..176 ",
            # res=[b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'],
            # responses=[b'176\x0020240303 03:56:49 GMT\x00']
            responses=[
                b"\x00\x00\x00*176\x0020240303 20:39:51 Greenwich Mean Time\x00"
            ],
        )

        # startApi message
        self.queue_transaction(
            request=b"\x00\x00\x00\x0871\x002\x001\x00\x00",
            # responses=[b"15\x001\x00DU1234567\x00", b"9\x001\x006\x00"],
            responses=[
                b"\x00\x00\x00\x0f15\x001\x00DU7606863\x00",
                b"\x00\x00\x00\x089\x001\x00530\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00\x00\x00\x0044\x002\x00-1\x002106\x00HMDS data farm connection is OK:ushmds\x00\x00"
                b"9\x001\x00530\x00",
            ],
        )


@pytest.mark.asyncio()
async def test_reconnect(connection):
    """
    tests connect then reconnect of the client
    """
    mock_socket = MockSocket()
    mock_socket.queue_handshake()
    with patch(
        "asyncio.open_connection",
        return_value=(mock_socket.mock_reader, mock_socket.mock_writer),
    ) as _:
        await connection.connect()

    assert connection._is_connected.is_set()

    mock_socket.queue_handshake()

    mock_socket.send_responses([b""])
    await asyncio.sleep(10)
    # await asyncio.sleep(25)


########################## G TESTS ############################


# def mock_handshake(connection):
#     # Arrange
#
#     handshake_responses = [
#         [b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'],
#         [b'15\x001\x00DU1234567\x00', b'9\x001\x006\x00'],
#     ]
#
#     def send_mocked_response(_):
#         responses = handshake_responses.pop(0)
#         while len(responses) > 0:
#             connection._handle_msg(responses.pop(0))
#
#     connection._sendMsg = Mock(side_effect=send_mocked_response)
#     return connection
#


@pytest.mark.asyncio()
async def test_connect(connection, mocker):
    # Arrange
    mock_reader = mocker.MagicMock()

    async def mocked_read(_):
        return b"nothing"  # do nothing

    mock_reader.read = mocked_read
    mock_writer = mocker.MagicMock()

    mocker.patch("asyncio.open_connection", return_value=(mock_reader, mock_writer))

    # Act
    await connection._connect()

    # Assert
    assert connection._reader == mock_reader
    assert connection._writer == mock_writer
    assert isinstance(connection._listen_task, asyncio.Task)
    assert (
        not connection._listen_task.done() and not connection._listen_task.cancelled()
    )
    assert connection._listen_task in asyncio.all_tasks(connection._loop)
    asyncio.open_connection.assert_called_once_with(connection._host, connection._port)


@pytest.mark.asyncio()
async def test_handshake(connection):
    sent_expected = [
        b"API\x00\x00\x00\x00\nv176..176 ",
        b"\x00\x00\x00\x0871\x002\x001\x00\x00",
    ]
    connection = mock_handshake(connection)

    # Assert
    await connection._handshake(timeout_seconds=1)
    assert connection.is_connected

    sent_messages: list[bytes] = [x[0][0] for x in connection._sendMsg.call_args_list]

    assert sent_messages == sent_expected

    # mocked_data = [b'4\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00']

    # Simulate receiving data from the mocked reader
    # mocked_response = b"Mocked response"
    # mock_reader.read.return_value = mocked_response
    # received_data = await connection._listen()
    # print(received_data)
    #
    #
    # Simulated data to receive
    # mocked_data = [b'176\x0020240229 12:41:55 Greenwich Mean Time\x00',
    #                b'15\x001\x00DU1234567\x00', b'9\x001\x006\x00']
    #
    # Mock the reader's recv method
    # mock_reader.recv.side_effect = mocked_data

    # Create connection object with mocked reader and writer
    # connection = MyConnection(mock_reader, mock_writer)

    # @pytest.mark.asyncio()
    # async def test_reconnect(self, client):
    #     await Connection.start_tws()
    #     await asyncio.sleep(15)

    #     # await client.connection.connect()
    #     await client.connection.start()

    #     await asyncio.sleep(5)
    #     await Connection.kill_tws()

    #     await asyncio.sleep(5)

    #     await Connection.start_tws()
    #     await asyncio.sleep(15)

    #     while True:
    #         await asyncio.sleep(0)

    # @pytest.mark.asyncio()
    # async def test_connect(self, client):
    #     """
    #     Can handshake be performed twice?
    #     """
    # await Connection.kill_tws()
    # client.socket.connect = Mock(side_effect=ConnectionRefusedError())
    # await Connection.start_tws()
    # await asyncio.sleep(15)

    # await Connection.start_tws()
    # return

    # await client.connect()

    # await client.connection.start()
    # await asyncio.sleep(4)
    # await client.connect()

    # await client.connect()

    # await asyncio.sleep(10)

    # @pytest.mark.asyncio()
    # async def test_disconnect_called(self, client):
    #     """
    #     Test that the client _handle_disconnect method is called when TWS disconnects
    #     """

    #     # await self.client.connect()

    #     pass

    # @pytest.mark.asyncio()
    # async def test_handle_disconnect(self):

    #     import os
    #     os.system("killall -m java")
    #     await asyncio.sleep(1)

    #     os.system("sh /opt/ibc/twsstartmacos.sh")

    #     await asyncio.sleep(5)

    # @pytest.mark.skip(reason="research")
    # @pytest.mark.asyncio()
    # async def test_reconnect_after_restart(self, client):
    #     """
    #     Does market data continue to continue streaming after restart? Conclusion: NO
    #     """

    #     instrument_id = InstrumentId.from_str("PL-PL.NYMEX")
    #     contract = instrument_id_to_contract(instrument_id)
    #     front_contract = await client.request_front_contract(contract)

    #     client.subscribe_bars(
    #         name=instrument_id.value,
    #         contract=front_contract,
    #         what_to_show=WhatToShow.BID,
    #         bar_size=BarSize._5_SECOND,
    #         callback= lambda x: print(x),
    #     )

    #     while True:
    #         await asyncio.sleep(0)
