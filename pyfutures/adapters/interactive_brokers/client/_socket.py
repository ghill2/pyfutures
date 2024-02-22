import asyncio
from collections.abc import Coroutine

from eventkit import Event
from ibapi import comm
from nautilus_trader.common.component import Logger


class Socket(asyncio.Protocol):
    """
    Event-driven socket connection.

    Events:
        * ``hasData`` (data: bytes):
          Emits the received socket data.
        * ``disconnected`` (msg: str):
          Is emitted on socket disconnect, with an error message in case
          of error, or an empty string in case of a normal disconnect.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        host: str,
        port: int,
        client_id: int,
        callback: Coroutine,
    ):
        self._log = Logger(type(self).__name__, logger)
        self._loop = loop
        self._host = host
        self._port = port
        self._client_id = client_id
        self._callback = callback

        self.is_ready = asyncio.Event()
        self.reset()
        self.disconnected = Event("disconnected")
        self._buf = b""

    def reset(self):
        self.transport = None
        self.numBytesSent = 0
        self.numMsgSent = 0

    async def connectAsync(self):
        if self.transport:
            # wait until a previous connection is finished closing
            if self.transport:
                self.transport.write_eof()
                self.transport.close()
            await self.disconnected

        self.reset()

        self.transport, _ = await self._loop.create_connection(lambda: self, self._host, self._port)

        self._log.debug("Socket connected")

    def isConnected(self):
        return self.transport is not None

    def sendMsg(self, msg):
        if self.transport:
            self.transport.write(msg)
            self.numBytesSent += len(msg)
            self.numMsgSent += 1

    def connection_lost(self, exc):
        self.transport = None
        msg = str(exc) if exc else ""
        self.disconnected.emit(msg)
        self._log.error(f"Connection lost {msg}")

    def data_received(self, data):
        self._buf += data

        while len(buf) > 0:
            (size, msg, buf) = comm.read_msg(buf)

            if not msg:
                self._log.debug("more incoming packet(s) are needed ")
                break

            self._log.debug(f"<-- {msg!r}")
            self._loop.run_until_complete(self._callback(msg))
            self._loop.run_until_complete(asyncio.sleep(0))

            # # wait for time and version response
            # msg = msg.decode(errors="backslashreplace")
            # fields = msg.split("\0")
            # fields.pop()
            # if len(fields) == 2:
            #     version = fields[0]
            #     assert int(version) == 176
            #     self.is_ready.set()
            #     continue


# class Socket(asyncio.Protocol):

#     def __init__(
#         self,
#         loop: asyncio.AbstractEventLoop,
#         logger: Logger,
#         host: str,
#         port: int,
#         client_id: int,
#         callback: Coroutine,
#     ):

#         self._loop = loop
#         self._host = host
#         self._port = port
#         self._client_id = client_id
#         self._callback = callback

#         self._listen_task = None
#         self._reader = None
#         self._writer = None
#         self._is_ready = asyncio.Event()

#     async def connect(self):

#         await self._reset()

#         # self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

#         # self._listen_task = self._loop.create_task(self._listen())

#         self._log.info("Connected")

#         await asyncio.wait_for(self._is_ready.wait(), 5)

#     def sendMsg(self, msg: bytes) -> None:

#         self._log.debug(f"--> {msg}")
#         self._writer.write(msg)
#         self._loop.create_task(self._writer.drain())

#     async def _reset(self):

#         self._log.debug("Resetting...")

#         if self._listen_task is not None:
#             self._listen_task.cancel()

#         if self._writer is not None:
#             self._writer.write_eof()
#             self._writer.close()
#             await self._writer.wait_closed()

#         self._listen_task = None
#         self._reader = None
#         self._writer = None

#         self._log.debug("Reset complete")

#     def data_received(self, data):

#     async def _listen(self) -> None:

#         assert self._reader is not None
#         assert self._listen_task is not None

#         buf = b""

#         self._log.info("Listen loop started")

#         while True:

#             data = await self._reader.read(4096)
#             buf += data

#             while len(buf) > 0:

#                 (size, msg, buf) = comm.read_msg(buf)

#                 if msg:
#                     self._log.debug(f"<-- {msg!r}")

#                     if self._is_ready.is_set():
#                         await self._callback(msg)
#                     else:
#                         # wait for time and version response
#                         msg = msg.decode(errors="backslashreplace")
#                         fields = msg.split("\0")
#                         fields.pop()
#                         if len(fields) == 2:
#                             version = fields[0]
#                             assert int(version) == 176
#                             self._is_ready.set()

#                     await asyncio.sleep(0)

#                 else:
#                     self._log.debug("more incoming packet(s) are needed ")
#                     break

#             await asyncio.sleep(0)
