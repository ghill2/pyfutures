import asyncio
import struct
import sys

from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import LoggerAdapter

from collections.abc import Coroutine

from ibapi import comm
from ibapi.wrapper import EWrapper
from typing import ValuesView
from typing import Coroutine
import socket

class Socket(asyncio.Protocol):
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        host: str,
        port: int,
        client_id: int,
        callback: Coroutine,
    ):
        
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._loop = loop
        self._host = host
        self._port = port
        self._client_id = client_id
        self._callback = callback
        
        self._listen_task = None
        self._reader = None
        self._writer = None
        self._is_ready = asyncio.Event()
        
    async def connect(self):
        self.socket = socket.socket()
        self.socket.connect((self.host, self.port))
        self.socket.settimeout(1)   #non-blocking
        
        await self._reset()
        
        # self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        
        self._listen_task = self._loop.create_task(self._listen())
        
        self._log.info("Connected")
        
        await asyncio.wait_for(self._is_ready.wait(), 5)
    
    def sendMsg(self, msg: bytes) -> None:
        
        self._log.debug(f"--> {msg}")
        self._writer.write(msg)
        self._loop.create_task(self._writer.drain())
        
    async def _reset(self):
        
        self._log.debug("Resetting...")
        
        if self._listen_task is not None:
            self._listen_task.cancel()

        if self._writer is not None:
            self._writer.write_eof()
            self._writer.close()
            await self._writer.wait_closed()
        
        self._listen_task = None
        self._reader = None
        self._writer = None
        
        self._log.debug("Reset complete")
    
    async def _listen(self) -> None:
        
        assert self._reader is not None
        assert self._listen_task is not None
        
        buf = b""

        self._log.info("Listen loop started")
            
        while True:
            
            data = await self._reader.read(4096)
            buf += data
            
            while len(buf) > 0:
                
                (size, msg, buf) = comm.read_msg(buf)
        
                if msg:
                    self._log.debug(f"<-- {msg!r}")
                    
                    if self._is_ready.is_set():
                        await self._callback(msg)
                    else:
                        # wait for time and version response
                        msg = msg.decode(errors="backslashreplace")
                        fields = msg.split("\0")
                        fields.pop()
                        if len(fields) == 2:
                            version = fields[0]
                            assert int(version) == 176
                            self._is_ready.set()
                            
                    await asyncio.sleep(0)
                    
                else:
                    self._log.debug("more incoming packet(s) are needed ")
                    break
                
            await asyncio.sleep(0)
                    
        
            
    
