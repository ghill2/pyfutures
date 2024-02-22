import asyncio
import functools
import socket
from collections.abc import Coroutine

from ibapi import comm
from nautilus_trader.common.component import Logger


class Socket(asyncio.Protocol):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        host: str,
        port: int,
        client_id: int,
        handler: Coroutine,
        disconnect_handler: Coroutine,
    ):
        self._log = Logger(type(self).__name__, logger)
        self._loop = loop
        self._host = host
        self._port = port
        self._client_id = client_id
        self._handler = handler

        self._listen_task = None
        self._is_ready = asyncio.Event()
        self._disconnect_handler = disconnect_handler

    def connect(self):
        if self._listen_task is not None:
            self._listen_task.cancel()

        self._socket = socket.socket()
        self._socket.connect((self._host, self._port))
        # self._socket.settimeout(1)   # non-blocking

        self._listen_task = self._loop.create_task(self._listen())

        self._log.info("Connected")

    def sendMsg(self, msg: bytes) -> None:
        self._log.debug(f"--> {msg}")
        self._socket.send(msg)

    async def _listen(self) -> None:
        buf = b""

        self._log.info("Listen loop started")

        while True:
            try:
                data = await self._loop.run_in_executor(None, functools.partial(self._socket.recv, 4096))
            except ConnectionResetError:
                """
                connection was reset by TWS
                """
                await self._disconnect_handler()

            # if data == b"":
            #     self._log.error("socket_disconnected")
            #     await self._disconnect_handler()

            buf += data

            while len(buf) > 0:
                (size, msg, buf) = comm.read_msg(buf)

                if msg:
                    self._log.debug(f"<-- {msg!r}")
                    await self._handler(msg)
                    await asyncio.sleep(0)
                else:
                    self._log.debug("more incoming packet(s) are needed ")
                    break

            await asyncio.sleep(0)
