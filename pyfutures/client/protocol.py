import asyncio
from pyfutures.logger import LoggerAdapter
from ibapi import comm
from ibapi.message import IN
from ibapi.message import OUT
import struct
from typing import Callable
from pathlib import Path
from pyfutures import PACKAGE_ROOT
import os
import pickle
import json
from pprint import pprint


# def parse_buffer(buf: bytes, cb: Callable):
#     """
#     Generator to parse all messages in the data / buffer
#     if the last message is incomplete, returns it
#     """
#


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
    def __init__(
        self,
        loop,
        connection_lost_callback: Callable,
        fields_received_callback: Callable,
        client_id: int = 1,
    ):
        self._loop = loop
        self.client_id = client_id
        self._connection_lost_callback = connection_lost_callback
        self._fields_received_callback = fields_received_callback

        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self._bstream = None
        self._buffer = b""

    def connection_made(self, transport):
        self._transport = transport
        print("connection made to: ", transport)

    def handle_message(self, msg: bytes):
        """
        receives a single complete message only at a time
        receives a null separated bytestring
        message = null separated ascii bytes with size prefix
        """
        fields = msg.split(b"\0")
        fields = tuple(fields[0:-1])
        if self._bstream is not None:
            ascii_fields = [f.decode("ascii") for f in fields]
            self._bstream[-1][1].append(ascii_fields)
        self._log.debug(f"<--- {fields}")
        self._fields_received_callback(fields)

    def data_received(self, data):
        try:
            self._log.debug("========== RESPONSE ==========")

            self._buffer += data

            self._log.debug(f"<-- buffer: {self._buffer}")

            while self._buffer:
                _, msg, self._buffer = comm.read_msg(self._buffer)
                if msg:
                    self.handle_message(msg)
                else:
                    break
        except Exception as e:
            self._log.exception("protocol data_received exception: ", e)

    def eof_received(self):
        self._log.error("eof received")

        if self._bstream is not None:
            self._bstream[-1][1].append(["eof"])

    def connection_lost(self, exc):
        self._log.error("connection lost")
        self._connection_lost_callback()

    def sendMsg(self, msg: str):
        """
        this function overrides eclient.sendMsg()
        it receives output from all ibapi requests methods
        and startApi
        """
        self._log.debug(f"--> sendMsg: {repr(msg)}")

        if self._bstream is not None:
            ascii_fields = msg.split("\x00")
            self._bstream.append([ascii_fields, []])

        msg_bytes = comm.make_msg(msg)
        self.write(msg_bytes)

    def write(self, msg: bytes):
        self._log.debug(f"--> {repr(msg)}")
        self._transport.write(msg)

    async def perform_handshake(self):
        # Handshake:
        # bytes are created directly inside ibapi._client connect()
        self.write(create_handshake())
        self.write(create_start_api(self.client_id))

        if self._bstream is not None:
            self._bstream.append([["handshake"], []])
            self._bstream.append([["startapi"], []])

    def enable_bytestrings(self):
        self._bstream: list[[str, list[str]]] = []

    def export_bytestrings(self, path: Path):
        pprint(self._bstream, indent=4)
        path = Path(path)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        self._log.debug(f"Exporting bytestrings to: {path}")
        with open(path, "w") as f:
            json.dump(self._bstream, f, indent=4)

        # self._is_connected_waiter = self._loop.create_future()
        # try:
        #
        #     await self._is_connected_waiter
        # finally:
        #     self._log.info("- Connected Successfully...")
        #     self._is_connected_waiter = None
        #
        # _buffer = parse_buffer(self._buffer, self.handle_message)
        # processed = 0
        # while processed < len(self._buffer) - 1:
        #     if len(self._buffer) < 4:
        #         break
        #     try:
        #         size = struct.unpack("!I", self._buffer[processed : processed + 4])[0]
        #
        #         chunk = self._buffer[processed + 4 : processed + 4 + size]
        #         print("CHUNK")
        #         print(chunk)
        #         msg = struct.unpack("!%ds" % size, chunk)[0]
        #
        #     except struct.error as e:
        #         if str(e).startswith("unpack requires a buffer of"):
        #             print(e)
        #         break
        #     else:
        #         print("DID NOT ERROR?")
        #         processed = processed + 4 + size
        #         self._buffer = self._buffer[processed:]
        #         self.handle_message(msg)

    # def export_bytestrings(self, path: Path):
    #     if self._file is not None:
    #         self._file.close()
    #         self._file = None
    #
    #     path = Path(path)
    #     path.parent.mkdir(parents=True, exist_ok=True)
    #
    #     if path.exists():
    #         self._log.warning("Previous bytestring file detected, removing file...")
    #         path.unlink()
    #
    #     self._file = open(path, "a")
    #
    #
    #
    # def __del__(self):
    #     if self._file is not None:
    #         self._file.close()
    #
    #     if self._file is not None:
    #         self._file.write(f"READ {data}\n")
    #         # data is only written when fileobject.close() is executed (on a graceful close)
    #         # this forces data to be written immediately
    #         os.fsync(self._file.fileno())
