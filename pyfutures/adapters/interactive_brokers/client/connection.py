"""
Event-driven socket connection.
"""

import asyncio
import struct
import sys

from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter


UNSET_INTEGER = 2**31 - 1
UNSET_DOUBLE = sys.float_info.max

from collections.abc import Coroutine

from ibapi import comm
from ibapi.connection import Connection as IBConnection
from ibapi.wrapper import EWrapper
from typing import ValuesView

class Connection:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        handler: Coroutine,
        wrapper: EWrapper,
        host: str,
        port: int,
        client_id: int,
        subscriptions: ValuesView,
    ):
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._loop = loop
        self._handler = handler
        
        self._listen_task: asyncio.Task | None = None # initialized on connect()
        self._watch_dog_task: asyncio.Task | None = None
        self.reset()
        self._wrapper = wrapper
        
        self._host = host
        self._port = port
        self._client_id = client_id
        self._subscriptions = subscriptions
        
    def reset(self):
        self._log.debug("Resetting...")

        self._accounts = None  # initialized on connect()
        self._hasReqId = False
        self._apiReady = False
        self._serverVersion = None
        self._handshake = asyncio.Future()

        if self._listen_task is not None:
            self._listen_task.cancel()
        self._listen_task = None
        
        if self._watch_dog_task is not None:
            self._watch_dog_task.cancel()
        self._watch_dog_task = None
        
        self._conn = None  # initialized on connect()
        
        self._log.debug("Reset complete")
    
    def is_connected(self) -> bool:
        """
        Returns False if the socket has been disconnected, for example, due to a schedule restart
        or during IB nightly reset.
        """
        return self._conn.isConnected()
    
    async def connect(self) -> None:
        
        self.reset()
        
        self._log.debug("Connecting socket...")
        await asyncio.wait_for(self._connect_socket(self._host, self._port), 4)
        self._log.debug("Connected")
        
        self._log.debug("Starting tasks...")
        self._listen_task = self._loop.create_task(self._listen())
        self._watch_dog_task = self._loop.create_task(self._run_watch_dog())
        self._log.debug("Started...")
        
        self._log.debug("Handshaking...")
        await self.handshake(self._host, self._port, self._client_id)
        self._log.debug("Handshaked")
        
    async def _connect_socket(self, host, port):

        self._conn = IBConnection(host, port)
        self._conn.connect()
        self._conn.wrapper = self._wrapper
        
    async def handshake(self):
        
        """
        Handshake the client with the trading server, required after connection
        """
        
        self._log.info(f"Connecting to {self._host}:{self._port} with clientId {self._clientId}...")

        msg = b"API\0" + self._prefix(b"v%d..%d%s" % (176, 176, b" "))

        self._conn.sendMsg(msg)

        await asyncio.wait_for(self._handshake, 3)

        self._log.info("API connection ready, server version")

        return self._handshake.result()
    
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
            
        
    async def _run_watch_dog(self):
        
        """
        Monitors the socket connection for disconnections.
        """
        
        try:
            
            while True:
                
                await asyncio.sleep(1)
                
                if self._conn.isConnected():
                    continue
                
                await self._handle_disconnect()
                
        except asyncio.CancelledError:
            self._log.debug("`watch_dog` task was canceled.")
                    
    async def _listen(self):
        
        self._log.info("Listen loop started")
        
        try:
            
            try:
                buf = b""
                while self._conn is not None and self._conn.isConnected():
                    
                    data = await self._loop.run_in_executor(None, self._conn.recvMsg)
                    buf += data
                    
                    while len(buf) > 0:
                        (size, msg, buf) = comm.read_msg(buf)
                        if msg:
                            await self._handle_msg(msg)
                        else:
                            self._log.debug("more incoming packet(s) are needed ")
                            break
                        
            except Exception as e:
                self._log.exception("unhandled exception in EReader worker ", e)
                
        except asyncio.CancelledError:
            self._log.debug("Message reader was canceled.")
            
    def sendMsg(self, msg: bytes):
        self._log.debug(f"--> {msg}")
        
        # TODO: catch Exceptions from ibapi related to writing to a closed transport
        self._conn.sendMsg(msg)

    async def _handle_msg(self, msg: bytes) -> None:
        
        self._log.debug(f"<-- {msg!r}")
        
        if not self._handshake.done():
            self._process_handshake(msg)
            return

        await self._handler(msg)

    def _process_handshake(self, msg: bytes):
        
        msg = msg.decode(errors="backslashreplace")
        fields = msg.split("\0")
        fields.pop()

        if not self._serverVersion and len(fields) == 2:
            version, _connTime = fields
            assert int(version) == 176
            self._serverVersion = version
            # send startApi message
            self.sendMsg(b"\x00\x00\x00\x0871\x002\x001\x00\x00")

            self._log.info("Logged on to server version 176")

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
                if self._hasReqId and self._accounts:
                    self._apiReady = True
                    self._handshake.set_result(None)

    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack(">I", len(msg)) + msg
    