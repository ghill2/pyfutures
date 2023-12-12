import asyncio
import struct
import sys
import functools
from nautilus_trader.common.logging import Logger
from nautilus_trader.common.logging import LoggerAdapter

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
        handler: Coroutine,
    ):
        
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._loop = loop
        self._host = host
        self._port = port
        self._client_id = client_id
        self._handler = handler
        
        self._listen_task = None
        self._is_ready = asyncio.Event()
        
    def connect(self):
        
        if self._listen_task is not None:
            self._listen_task.cancel()
        
        self._socket = socket.socket()
        self._socket.connect((self._host, self._port))
        self._socket.settimeout(1)   #non-blocking
        
        self._listen_task = self._loop.create_task(self._listen())
        
        self._log.info("Connected")
    
    def sendMsg(self, msg: bytes) -> None:
        self._log.debug(f"--> {msg}")
        self._socket.send(msg)
        
    async def _listen(self) -> None:
        
        buf = b""

        self._log.info("Listen loop started")
            
        while True:
            
            data = await self._loop.run_in_executor(
                                None,
                                functools.partial(self._socket.recv, 4096)
                            )
            
            buf += data
            
            print(buf)
            
            while len(buf) > 0:
                
                (size, msg, buf) = comm.read_msg(buf)
        
                if msg:
                    self._log.debug(f"<-- {msg!r}")
                    
                    if self._is_ready.is_set():
                        await self._handler(msg)
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
                    
        
            
    
