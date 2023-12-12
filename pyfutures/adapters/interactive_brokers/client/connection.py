import asyncio
import struct
import sys
import psutil
import os
from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter

from collections.abc import Coroutine

from ibapi import comm
from ibapi.wrapper import EWrapper
from typing import ValuesView
from pyfutures.adapters.interactive_brokers.client.socket import Socket
from pyfutures.adapters.interactive_brokers.client.handshake import Handshake
                    
class Connection:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        handler: Coroutine,
        host: str,
        port: int,
        client_id: int,
        subscriptions: ValuesView,
    ):
        
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._loop = loop
        self._handler = handler
        
        self._watch_dog_task: asyncio.Task | None = None
        
        self.socket = Socket(
            loop=self._loop,
            logger=logger,
            host=host,
            port=port,
            client_id=client_id,
            handler=self._handle_msg,
        )
        
        self._handshake = Handshake(
            socket=self.socket,
            logger=logger,
        )
        
        self._host = host
        self._port = port
        self._client_id = client_id
        self._subscriptions = subscriptions
        
        self.reset()
        
    def reset(self):
        
        self._log.debug("Resetting...")

        if self._watch_dog_task is not None:
            self._watch_dog_task.cancel()
        self._watch_dog_task = None
        
        self._log.debug("Reset complete")
    
    async def connect(self, attempts: int = 5) -> bool:
        
        # Wait for TWS to start
        while not self.is_tws_running():
            self._log.debug(f"Waiting for tws to start...")
            await asyncio.sleep(2.5)
        
        # connect socket    
        while attempts > 0:
            
            self._log.debug(f"Attempt {attempts}...")
            try:
                self.socket.connect()
                break
            except ConnectionRefusedError as e:
                """
                When TWS is not running, the socket connection will be refused.
                """
                self._log.error(f"{repr(e)}")
                self._log.debug(f"Socket connection refused. Waiting 10 seconds...")
                attempts -= 1
                await asyncio.sleep(10)
        
        # handshake
        while attempts > 0:
            pass
        
    async def _handle_msg(self, msg: bytes) -> None:
        
        self._log.debug(f"{msg!r}")
        if not self._handshake.is_completed:
            self._handshake.process(msg)
            return
        
        
    async def _run_watch_dog(self):
        
        """
        Monitors the socket connection for disconnections.
        """
        
        try:
            
            while True:
                
                await asyncio.sleep(1)
                
                if self.is_connected:
                    continue
                
                await self._handle_disconnect()
                
        except asyncio.CancelledError:
            self._log.debug("`watch_dog` task was canceled.")
    
    async def _handle_disconnect(self):
        """
        Called when the socket has been disconnected for some reason, for example,
        due to a schedule restart or during IB nightly reset.
        """
        self._log.debug("Server has been disconnected, attempting to reconnect...")
        
        success = False
        while not success:
            success = await self.connect()
        
        # reconnect subscriptions
        # for sub in self._subscriptions:
        #     sub.cancel()
    
    @classmethod
    async def start_tws(cls):
        if cls.is_tws_running():
            await cls.kill_tws()
        os.system("sh /opt/ibc/twsstartmacos.sh")
        while not cls.is_tws_running():
            await asyncio.sleep(0.25)
            
    @classmethod
    async def kill_tws(cls):
        os.system("killall -m java")
        os.system("killall -m Trader Workstation 10.26")
        while cls.is_tws_running():
            await asyncio.sleep(0.25)
        
    @staticmethod
    def is_tws_running() -> bool:
        for process in psutil.process_iter(['pid', 'name']):
            name = process.info['name'].lower()
            if name == "java" or name.startswith("Trader Workstation"):
                return True
        return False
    

    
    
    # def is_connected(self) -> bool:
    #     """
    #     Returns False if the socket has been disconnected, for example, due to a schedule restart
    #     or during IB nightly reset.
    #     """
    #     return self._conn.isConnected()
        
        # return
        # try:
        #     await asyncio.wait_for(self.socket.is_ready.wait(), 5)
        #     return
        # except asyncio.TimeoutError as e:
        #     self._log.error(f"{repr(e)}")
            
            
        # # except Exception as e:
        # #     self._log.error(type(e))
        # #     self._log.error(repr(e))
        # #     pass
            
            
        # while attempts > 0:
            
        #     # connect socket
        #     self._log.debug(f"Connecting socket...")
        #     try:
        #         await asyncio.wait_for(self.socket.connect(), 10)
        #         await asyncio.wait_for(self._handshake.perform(), 10)
        #     except (asyncio.TimeoutError, ConnectionRefusedError, ConnectionResetError) as e:
        #         self._log.error(repr(e))
        #         self._log.debug(f"Error during socket connection, reconnecting... attempts={attempts}")
        #         attempts -= 1
        #         await asyncio.sleep(5)
        #         continue
            
        #     return True
        
        # self._log.error("Failed to connect")
        # return False