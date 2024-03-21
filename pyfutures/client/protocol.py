import asyncio
from pyfutures.logger import LoggerAdapter
from ibapi import comm
from ibapi.message import IN
from ibapi.message import OUT
import struct
from typing import Callable
from pathlib import Path
from pyfutures import PACKAGE_ROOT


def create_handshake() -> bytes:
    _min_version = 176
    _max_version = 176
    msg = b"v%d..%d" % (_min_version, _max_version)
    msg = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
    msg = b"API\x00" + msg
    return msg


def create_start_api(client_id) -> bytes:
    msg = b"71\x002\x00" + str(client_id).encode() + b"\x00\x00"
    msg = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
    return msg


class Protocol(asyncio.Protocol):
    def __init__(self, loop, client_id, connection_lost_callback: Callable, fields_received_callback: Callable):
        self._loop = loop
        self.client_id = client_id
        self._connection_lost_callback = connection_lost_callback
        self._fields_received_callback = fields_received_callback

        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self.export = False

    def connection_made(self, transport):
        self._transport = transport
        print("connection made to: ", transport)

    def data_received(self, data):
        self._log.debug("========== RESPONSE ==========")
        self._log.debug(f"<-- {data}")
        if self.export:
            self.file.write(f"('IN', {data})")
        start = 0
        while start < len(data) - 1:
            size = struct.unpack("!I", data[start : start + 4])[0]
            self._log.debug(f"read_msg: size: {size}")
            end = start + 4 + size
            buf = struct.unpack("!%ds" % size, data[start + 4 : end])[0]
            start = end
            fields = buf.split(b"\0")
            fields = tuple(fields[0:-1])
            self._log.debug(f"<--- {fields}")
            self._fields_received_callback(fields)

    def eof_received(self):
        self._log.error("eof received")

    def connection_lost(self, exc):
        self._log.exception("connection lost", exc)
        self._connection_lost_callback()

    def write(self, msg: bytes):
        self._log.debug(f"--> {repr(msg)}")
        if self.export:
            self.file.write(f"('OUT', {msg})")
        self._transport.write(msg)

    async def perform_handshake(self):
        self.write(create_handshake())
        self.write(create_start_api(self.client_id))

        self._is_connected_waiter = self._loop.create_future()
        try:
            await self._is_connected_waiter
        finally:
            self._log.info("- Connected Successfully...")
            self._is_connected_waiter = None

    def export_bytestrings(self, path: Path):
        self.export = True
        self.file = open(path, "w")  # Open the file in write mode
        self.file.write("BYTES = [")

    def __del__(self):
        if self.export:
            self.file.write("\n]")
            self.file.close()
