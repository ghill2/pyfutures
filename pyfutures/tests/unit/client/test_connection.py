import asyncio

import pytest

from pyfutures.client.connection import Connection
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import MagicMock
from unittest.mock import patch
import pandas as pd
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.enums import Frequency
from pyfutures.adapter.enums import WhatToShow
from ibapi.contract import Contract as IBContract

pytestmark = pytest.mark.unit

class TestConnection:
    
    @pytest.mark.asyncio()
    async def test_connect(self, connection):
        
        # Arrange
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()
        
        with patch(
            "asyncio.open_connection",
            return_value=(mock_reader, mock_writer),
        ):
        
            # Act
            await connection._connect()
            
            # Assert
            asyncio.open_connection.assert_called_once_with(connection._host, connection._port)
            assert connection._reader == mock_reader
            assert connection._writer == mock_writer
            
            listen_started = not connection._listen_task.done() and not connection._listen_task.cancelled()
            assert listen_started
            assert connection._listen_task in asyncio.all_tasks(connection._loop)

    @pytest.mark.asyncio()
    async def test_handshake(self, connection, mock_socket):
        
        # Act
        with patch(
            "asyncio.open_connection",
            return_value=(mock_socket.mock_reader, mock_socket.mock_writer),
        ):
            
            await connection._connect()
            await connection._handshake(timeout_seconds=0.5)
            assert connection.is_connected
    
    @pytest.mark.asyncio()
    async def test_empty_byte_handles_disconnect(self, connection):
        
        connection._handle_disconnect = AsyncMock()
        mock_reader = AsyncMock()
        mock_reader.read.return_value = b""
        
        with patch(
            "asyncio.open_connection",
            return_value=(mock_reader, None),
        ):
            await connection._connect()  # start listen task
            await asyncio.sleep(0)
            connection._handle_disconnect.assert_called_once()
            
    @pytest.mark.asyncio()
    async def test_connection_reset_error_handles_disconnect(self, connection):
        
        connection._handle_disconnect = AsyncMock()
        mock_reader = AsyncMock()
        
        def raise_error(*args, **kwargs):
            raise ConnectionResetError()
        
        mock_reader.read.side_effect = raise_error
        
        with patch(
            "asyncio.open_connection",
            return_value=(mock_reader, None),
        ):
            await connection._connect()  # start listen task
            await asyncio.sleep(0)
            connection._handle_disconnect.assert_called_once()
            
    @pytest.mark.asyncio()
    async def test_disconnect_resets(self, connection, mock_socket):
        
        with patch(
            "asyncio.open_connection",
            return_value=(mock_socket.mock_reader, mock_socket.mock_writer),
        ):
            await connection.connect()
            assert connection.is_connected
            await connection._handle_disconnect()
            
            assert not connection._is_connected.is_set()
            assert len(connection._handshake_message_ids) == 0
            assert connection._reader is None
            assert connection._writer  is None
            assert connection._listen_task is None
            assert not connection.is_connected
            assert connection._monitor_task is not None
            
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_connect_after_disconnect(self, connection, mock_socket):
        
        with patch(
            "asyncio.open_connection",
            return_value=(mock_socket.mock_reader, mock_socket.mock_writer),
        ):
            
            await connection.connect()
            assert connection.is_connected
            
            mock_socket.disconnect()
            await asyncio.sleep(1)
            assert not connection.is_connected
        
            
            # await connection.connect()
            # assert connection.is_connected
    
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
