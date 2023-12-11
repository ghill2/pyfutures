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


class Connection:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        logger: Logger,
        handler: Coroutine,
    ):
        self._log = LoggerAdapter(type(self).__name__, logger)
        self._loop = loop
        self._handler = handler

    async def reset(self):
        self._log.debug("Resetting...")

        self._accounts = None  # initialized on connect()
        self._hasReqId = False
        self._apiReady = False
        self._serverVersion = None
        self._handshake = asyncio.Future()

        if getattr(self, "_listen_task", None) is not None:
            self._listen_task.cancel()

        if getattr(self, "_writer", None) is not None:
            self._writer.write_eof()
            self._writer.close()
            await self._writer.wait_closed()

        self._listen_task = None
        self._reader = None
        self._writer = None

        self._log.debug("Reset complete")

    async def connect(self, host, port):
        await self.reset()

        self._reader, self._writer = await asyncio.open_connection(host, port)

        self._listen_task = self._loop.create_task(self._listen())

        self._log.info("Connected")

    async def handshake(self, host, port, clientId, timeout=2.0):
        try:
            self._log.info(f"Connecting to {host}:{port} with clientId {clientId}...")

            msg = b"API\0" + self._prefix(b"v%d..%d%s" % (176, 176, b" "))

            self.sendMsg(msg)

            await asyncio.wait_for(self._handshake, 3)

            self._log.info("API connection ready, server version")

            return self._handshake.result()

        except BaseException as e:
            # self.disconnect()
            msg = f"API connection failed: {e!r}"
            self._log.error(msg)

            if isinstance(e, ConnectionRefusedError):
                self._log.error("Make sure API port on TWS/IBG is open")

            raise

    async def _listen(self):
        assert self._reader is not None

        buf = b""

        self._log.info("Listen loop started")

        # try:
        while True:
            data = await self._reader.read(4096)
            buf += data

            # self._log.debug(f"reader loop, recvd size {buf}")

            while len(buf) > 0:
                (size, msg, buf) = comm.read_msg(buf)

                if msg:
                    await self._handle_msg(msg)
                    await asyncio.sleep(0)
                else:
                    self._log.debug("more incoming packet(s) are needed ")
                    break

    def sendMsg(self, msg: bytes):
        self._log.debug(f"--> {msg}")
        self._writer.write(msg)
        self._loop.create_task(self._writer.drain())

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
                    self._handshake.set_result(self._accounts)

    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack(">I", len(msg)) + msg