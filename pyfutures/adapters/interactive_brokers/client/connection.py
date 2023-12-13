import asyncio
import struct
import functools
import sys
import psutil
import os
from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter

from collections.abc import Coroutine

from ibapi import comm
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection as IBConnection
from typing import ValuesView
from pyfutures.adapters.interactive_brokers.client.socket import Socket
                    
class Connection:
    
    (DISCONNECTED, CONNECTING, CONNECTED) = range(3)
    
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
        
        self.socket = IBConnection(
            # loop=self._loop,
            # logger=logger,
            host=host,
            port=port,
            # client_id=client_id,
            # handler=self._handle_msg,
            # disconnect_handler=self._handle_disconnect,
        )
        self.socket.wrapper = self

        self._host = host
        self._port = port
        self._client_id = client_id
        self._subscriptions = subscriptions
        self._listen_task = None
        self._connection_task = None
        self.is_ready = asyncio.Event()
        self.connection_state = Connection.DISCONNECTED
        
    async def _listen(self) -> None:
        
        assert self._reader is not None
        
        buf = b""

        self._log.info("Listen loop started")
        
        try:
            try:
                while True:
                    
                    # data = await self._loop.run_in_executor(None, self.socket.recvMsg)
                    data = await self._reader.read(4096)
                    
                    buf += data
                    
                    while len(buf) > 0:
                        
                        (size, msg, buf) = comm.read_msg(buf)
                
                        if msg:
                            await self._handle_msg(msg)
                            await asyncio.sleep(0)
                        else:
                            self._log.debug("more incoming packet(s) are needed ")
                            break
                        
                    await asyncio.sleep(0)
            except Exception as e:
                self._log.error(repr(e))
                
        except asyncio.CancelledError:
            
            self._log.debug("`watch_dog` task was canceled.")
                
    async def _run_watch_dog(self):
        
        """
        Monitors the socket connection for disconnections.
        """
        try:
            
            while True:
                
                if self.socket.isConnected():
                    continue
                
                if self.connection_state == Connection.DISCONNECTED:
                    continue
                
                self._log.debug(f"Watchdog found the socket disconnected. Reconnecting...")
                
                await self.connect()
                
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            self._log.debug("`watch_dog` task was canceled.")
    
    def connectionClosed(self):
        self._log.error(f"Connection closed")
        self.connection_state = Connection.DISCONNECTED
                
    async def start(self) -> bool:
        
        if self._watch_dog_task is not None:
            self._watch_dog_task.cancel()
        self._watch_dog_task = self._loop.create_task(self._run_watch_dog())
        
    async def connect(self) -> bool:
       
        self._log.debug("Connecting...")
        
        if self._connection_task is not None:
            self._connection_task.cancel()
            
        self._connection_task = self._loop.create_task(self._connect())
            
    async def _connect(self) -> bool:
        
        self.connection_state = Connection.CONNECTING
        
        if self._listen_task is not None:
            self._listen_task.cancel()
            
        self._log.debug(f"Connecting socket")
        
        try:
            self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        except ConnectionRefusedError as e:
            self._log.error(repr(e))
            self.connection_state = Connection.DISCONNECTED
            return False
        
        self._log.debug(f"Starting listen task")
        self._listen_task = self._loop.create_task(self._listen())
        self._log.debug(f"Performing handshake")
        
        try:
            await self.perform_handshake()
        except asyncio.TimeoutError:
            self._log.error("Handshake failed")
            self.connection_state = Connection.DISCONNECTED
            return False
        
    def sendMsg(self, msg: bytes) -> None:
        
        if self._writer is None:
            self._log.error(f"A message was sent when the Connection was disconnected.")
            return
        
        self._log.debug(f"--> {msg}")
        self._writer.write(msg)
        self._loop.create_task(self._writer.drain())

    async def _handle_msg(self, msg: bytes) -> None:
        
        self._log.debug(f"<-- {msg!r}")
        
        if not self.is_ready.is_set():
            self._process_handshake(msg)
            return
    
    async def _handle_disconnect(self):
        """
        Called when the socket has been disconnected for some reason, for example,
        due to a schedule restart or during IB nightly reset.
        """
        self._log.debug("Server has been disconnected, attempting to reconnect...")
        
        await self.connect()
        
        # reconnect subscriptions
        # for sub in self._subscriptions:
        #     sub.cancel()
    
    async def perform_handshake(self) -> None:
        
        self.is_ready.clear()
        self._accounts = None
        self._hasReqId = False
        self._apiReady = False
        self._serverVersion = None
        
        self._log.info(f"Handshaking...")

        msg = b"API\0" + self._prefix(b"v%d..%d%s" % (176, 176, b" "))
        
        self.sendMsg(msg)
        
        self._log.debug("Waiting for handshake response")

        await asyncio.wait_for(self.is_ready.wait(), 10)

        self._log.info("API connection ready, server version 176")

    def _process_handshake(self, msg: bytes):
        
        self._log.debug(f"Processing handshake message {msg}")
        
        msg = msg.decode(errors="backslashreplace")
        fields = msg.split("\0")
        fields.pop()

        if not self._serverVersion and len(fields) == 2:
            version, _connTime = fields
            assert int(version) == 176
            self._serverVersion = version
            # send startApi message
            self._log.debug(f"Sending startApi message...")
            self.sendMsg(b"\x00\x00\x00\x0871\x002\x001\x00\x00")
        else:
            if not self._apiReady:
                # snoop for nextValidId and managedAccounts response,
                # when both are in then the client is ready
                msgId = int(fields[0])
                if msgId == 9:
                    _, _, validId = fields
                    # self.updateReqId(int(validId))
                    # self.wrapper.nextValidID()
                    self._hasReqId = True
                elif msgId == 15:
                    _, _, accts = fields
                    self._accounts = [a for a in accts.split(",") if a]
                if self._hasReqId and self._accounts is not None:
                    self._apiReady = True
                    self.is_ready.set()
        
    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack(">I", len(msg)) + msg
    
    @classmethod
    async def start_tws(cls):
        print("Starting tws...")
        if cls.is_tws_running():
            await cls.kill_tws()
        os.system("sh /opt/ibc/twsstartmacos.sh")
        
        while not cls.is_tws_running():
            print("Waiting for tws to open...")
            await asyncio.sleep(1)
            
    @classmethod
    async def kill_tws(cls):
        print("Killing tws...")
        os.system("killall -m java")
        os.system("killall -m Trader Workstation 10.26")
        while cls.is_tws_running():
            print("Waiting for tws to close...")
            await asyncio.sleep(1)
        
    @staticmethod
    def is_tws_running() -> bool:
        for process in psutil.process_iter(['pid', 'name']):
            name = process.info['name'].lower()
            if name == "java" or name.startswith("Trader Workstation"):
                return True
        return False
    
    def error(  # noqa: C901 too complex
        self,
        req_id: int,
        error_code: int,
        error_string: str,
        advanced_order_reject_json: str = "",
    ) -> None:
        self._log.debug(error_string)
    
    
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
                # # Wait for TWS to start
        # while not self.is_tws_running():
        #     self._log.debug(f"Waiting for tws to start...")
        #     await asyncio.sleep(2.5)
                # connect socket
        # while attempts > 0:
            
        #     self._log.debug(f"Attempt {attempts}...")
        #     try:
                
        #         break
        #     except ConnectionRefusedError as e:
        #         """
        #         When TWS is not running, the socket connection will be refused.
        #         """
        #         self._log.error(f"{repr(e)}")
        #         self._log.debug(f"Socket connection refused. Waiting 10 seconds then reattempting...")
        #         attempts -= 1
        #         await asyncio.sleep(10)
        
        
        # return
            
        # # # handshake
        # # while attempts > 0:
        # #     self._log.debug(f"Attempt {attempts}...")
        # #     try:
                
        # #     except asyncio.TimeoutError as e:
        # #         """
        # #         When TWS is not running, the socket connection will be refused.
        # #         """
        # #         self._log.error(f"{repr(e)}")
        # #         self._log.debug(f"Handshake failed. Waiting 10 seconds then reattempting...")
        # #         attempts -= 1
        # #         await asyncio.sleep(10)
        
        # self._log.debug("Connection failed")
        