import asyncio

import pytest

from pyfutures.adapters.interactive_brokers.client.connection import Connection

from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from unittest.mock import AsyncMock
init_logging(level_stdout=LogLevel.DEBUG)



class TestInteractiveBrokersClient:
    
    @pytest.mark.asyncio()
    async def test_connect_and_handshake(self, connection):
        
        await connection.connect()
        
        # assert isinstance(connection._reader, asyncio.streams.StreamReader)
        # assert isinstance(connection._writer, asyncio.streams.StreamWriter)
        # task_names = [t.name for t in asyncio.all_tasks(connection._loop)]
        # assert "listen" in task_names
        
        # connection._sendMsg = AsyncMock(
            
        # )
        
        
        # mocked = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
        # asyncio.open_connection = mocked
        
        # mocked.assert_called_once_with("127.0.0.1", 4002)
        
    @pytest.mark.asyncio()
    async def test_reconnect(self, client):
        await Connection.start_tws()
        await asyncio.sleep(15)

        # await client.connection.connect()
        await client.connection.start()

        await asyncio.sleep(5)
        await Connection.kill_tws()

        await asyncio.sleep(5)

        await Connection.start_tws()
        await asyncio.sleep(15)

        while True:
            await asyncio.sleep(0)

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
