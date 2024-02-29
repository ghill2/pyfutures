import asyncio

import pytest

from pyfutures.adapters.interactive_brokers.client.connection import Connection

from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from unittest.mock import AsyncMock
from unittest.mock import Mock
init_logging(level_stdout=LogLevel.DEBUG)

class TestConnection:
    
    @pytest.mark.asyncio()
    async def test_connect(self, connection, mocker):
        
        # Arrange
        mock_reader = mocker.MagicMock()
        async def mocked_read(_):
            return b"nothing"  # do nothing
        mock_reader.read = mocked_read
        mock_writer = mocker.MagicMock()
        
        mocker.patch('asyncio.open_connection', return_value=(mock_reader, mock_writer))
        
        # Act
        await connection._connect()
        
        # Assert
        assert connection._reader == mock_reader
        assert connection._writer == mock_writer
        assert isinstance(connection._listen_task, asyncio.Task)
        assert not connection._listen_task.done() and not connection._listen_task.cancelled()
        assert connection._listen_task in asyncio.all_tasks(connection._loop)
        asyncio.open_connection.assert_called_once_with(connection._host, connection._port)
    
    @pytest.mark.asyncio()
    async def test_handshake(self, connection):
        
        # Arrange
        sent_expected = [
            b'API\x00\x00\x00\x00\nv176..176 ',
            b'\x00\x00\x00\x0871\x002\x001\x00\x00',
        ]
        handshake_responses = [
            [b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'],
            [b'15\x001\x00DU1234567\x00', b'9\x001\x006\x00'],
        ]
        
        def send_mocked_response(_):
            responses = handshake_responses.pop(0)
            while len(responses) > 0:
                connection._handle_msg(responses.pop(0))
                
        connection._sendMsg = Mock(side_effect=send_mocked_response)
        
        # Assert
        await connection._handshake(timeout_seconds=1)
        assert connection.is_connected
        
        sent_messages: list[bytes] = [x[0][0] for x in connection._sendMsg.call_args_list]
        
        assert sent_messages == sent_expected
        
        
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
