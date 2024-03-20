import asyncio
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
from asyncio.protocols import Protocol
from ibapi.decoder import Decoder
from pyfutures.logger import LoggerAdapter
from ibapi import comm
from ibapi.message import IN
from ibapi.message import OUT
import struct


def create_handshake():
    _min_version = 176
    _max_version = 176
    msg = b"v%d..%d" % (_min_version, _max_version)
    msg = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
    msg = b"API\x00" + msg
    return handshake_bytes


class IBProtocol(Protocol):
    def __init__(self, loop, client):
        self._loop = loop
        self._decoder = Decoder(serverVersion=176, wrapper=client)
        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self._reset()

    def _reset(self):
        # self._buffer = bytearray()  # read buffer
        # self._handshake = self._loop.create_future()
        # self._startapi = self._loop.create_future()
        self._is_connected_waiter = None

    def connection_made(self, transport):
        self._transport = transport
        print("connection made")

    def data_received(self, data):
        self._log.debug("========== RESPONSE ==========")
        self._log.debug(f"<-- {data}")
        start = 0
        while start < len(data) - 1:
            size = struct.unpack("!I", data[start : start + 4])[0]
            print(data[start : start + size])
            self._log.debug(f"read_msg: size: {size}")
            end = start + 4 + size
            buf = struct.unpack("!%ds" % size, data[start + 4 : end])[0]
            start = end
            fields = buf.split(b"\0")
            # del fields[-1]
            fields = tuple(fields[0:-1])
            self._log.debug(f"<--- {fields}")
            self._decoder.interpret(fields)
        return fields

    def eof_received(self):
        self._log.error("eof received")

    def connection_lost(self, exc):
        # reconnect
        self._log.exception("connection lost", exc)

    def sendMsg(self, msg: bytes) -> None:
        """
        messages output from self.eclient are sent here
        """
        self._log.debug(f"--> {repr(msg)}")
        self._transport.write(msg)

    async def connect(self, client_id):
        self.sendMsg(b"API\x00\x00\x00\x00\tv176..176")

        self.sendMsg(b"\x00\x00\x00\t71\x002\x0010\x00\x00")

        self._is_connected_waiter = self._loop.create_future()
        try:
            print("BEFORE AWAITER")
            await self._is_connected_waiter
        finally:
            self._log.info("- Connected Successfully...")
            self._is_connected_waiter = None


# async def main():
#     host = "127.0.0.1"
#     port = 4002
#     _loop = asyncio.get_running_loop()
#     _protocol = IBProtocol()
#     _transport, _ = await _loop.create_connection(lambda: _protocol, host, port)
#     _transport.write(b"API\x00\x00\x00\x00\tv176..176")
#     await asyncio.sleep(20)
#
#
# asyncio.run(main())
