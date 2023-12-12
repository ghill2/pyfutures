import asyncio
import struct
import sys

from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter

from collections.abc import Coroutine

from ibapi import comm
from ibapi.wrapper import EWrapper
from typing import ValuesView
from typing import Coroutine
from pyfutures.adapters.interactive_brokers.client.socket import Socket

class Handshake:
    
    def __init__(
        self,
        socket: Socket,
        logger: Logger,
    ):
        self._socket = socket
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._reset()
    
    @property
    def is_completed(self) -> bool:
        return self._is_ready.is_set()
        
    async def perform(self) -> None:
        
        self._reset()
        
        self._log.info(f"Handshaking...")

        msg = b"API\0" + self._prefix(b"v%d..%d%s" % (176, 176, b" "))

        self._socket.sendMsg(msg)

        await asyncio.wait_for(self._is_ready.wait(), 3)

        self._log.info("API connection ready, server version")

    def process(self, msg: bytes):
        
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
            self._socket.sendMsg(b"\x00\x00\x00\x0871\x002\x001\x00\x00")
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
                    self._is_ready.set()
    
    def _reset(self) -> None:
        
        self._is_ready = asyncio.Event()
        self._accounts = None
        self._hasReqId = False
        self._apiReady = False
        self._serverVersion = None
        
    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack(">I", len(msg)) + msg