import asyncio
import time
from decimal import Decimal
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.adapters.interactive_brokers.parsing import instrument_id_to_contract
from pyfutures.adapters.interactive_brokers.client.connection import Connection

class TestInteractiveBrokersClient:
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