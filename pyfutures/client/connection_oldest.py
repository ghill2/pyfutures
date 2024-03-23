import asyncio
import os
import struct
from collections.abc import Coroutine

import psutil
from ibapi import comm

from pyfutures.logger import LoggerAdapter


# this bug is still present on the gnznz fork:
# https://github.com/UnusualAlpha/ib-gateway-docker/issues/88


# class Connection:
#
#
#     def __init__(
#         self,
#         loop: asyncio.AbstractEventLoop,
#         host: str,
#         port: int,
#         client_id: int,
#         subscriptions: dict = {},
#     ):
#         self.loop = loop
#         self.host = host
#         self.port = port
#         self._subscriptions = subscriptions
#         self.client_id = client_id
#
#         self._log = LoggerAdapter.from_attrs(name=type(self).__name__)
#         # self._handlers = set()
#
#         # attributes that reset
#         # self._reader, self._writer = (None, None)
#         # self._handshake_message_ids = []
#
#
#     def _reset(self) -> bool:
#         self._log.debug("Initializing...")
#
#         # self._is_connected.clear()
#         # self._handshake_message_ids.clear()
#         self._reader, self._writer = (None, None)
#
#         self._log.debug("Initializing listen task...")
#         if self._listen_task is not None:
#             self._listen_task.cancel()
#         self._listen_task = None
#
#         # close the writer
#         try:
#             if self._writer is not None and not self._writer.is_closing():
#                 self._writer.write_eof()
#                 self._writer.close()
#                 # await self._writer.wait_closed()
#         except RuntimeError as e:
#             if "unable to perform operation on" in str(e) and "closed=True" in str(e):
#                 pass
#             else:
#                 raise
#
#         self._log.debug("Successfully Initialized...")

# def register_handler(self, handler: Coroutine) -> None:
#     self._handlers.add(handler)

# async def _listen(self) -> None:
#     assert self._reader is not None

# async def _handle_discconnect(self) -> None:
#     """
#     Called when the socket has been disconnected for some reason, for example,
#     due to a schedule restart or during IB nightly reset.
#     """
#     self._log.debug("Handling disconnect.")
#
#     self._connect_monitor_task.cancel()
#     self._connect_monitor_task = None
#
# reconnect subscriptions
# for sub in self._subscriptions:
#     sub.cancel()

# def _handle_msg(self, msg: bytes) -> None:
#     if self.is_connected:
#         for handler in self._handlers:
#             handler(msg)
#     else:
#         self._process_handshake(msg)

#     async def _connect_monitor(self, timeout_seconds: float | int = 5.0) -> None:
#         """
#         Monitors the socket connection for disconnections.
#         """
#         self._log.debug("Connect Monitor task started...")
#         try:
#             while True:
# ;        except Exception as e:
#             self._log.exception("_connect_monitor task exception, task cancelled", e)

# @property
# def is_connected(self) -> bool:
#     return self._is_connected.is_set()
#
# async def start(self) -> bool:
#     self._log.debug("Starting...")

# async def create_connection(self, host, port):
#     """
#     In a separate function for mocking / testing purposes
#     """
#     return await asyncio.open_connection(self.host, self.port)
#
# async def connect(self, timeout_seconds: int = 5) -> None:
#     """
#     Called by the user after instantiation
#     """

# if self._connect_monitor_task is None:
# self._connect_monitor_task = self.loop.create_task(self._connect_monitor(timeout_seconds=timeout_seconds))

# await asyncio.wait_for(self._is_connected.wait(), timeout_seconds)

# async def _connect(self) -> None:
#     """
#     Called by watch_dog and manually with connect() by the user
#     NOTE: do not call handshake here so we can test connect and handshake separately
#     """
# self._log.debug(f"Connecting on client_id {self.client_id}...")

# self._reset()

# connect socket
# self._log.debug("Connecting socket...")
# try:
#     self._reader, self._writer = await self.create_connection(self.host, self.port)
# except ConnectionRefusedError as e:
#     self._log.error(f"ConnectionRefusedError: Socket connection failed, check TWS is open {e!r}")
#     return
# self._log.debug(f"Socket connected. {self._reader} {self._writer}")

# start listen task
# self._log.debug("Starting listen task...")
# self._listen_task = self.loop.create_task(self._listen(), name="listen")
# self._log.info("Listen task started")

# async def _handshake(self, timeout_seconds: float | int = 5.0) -> None:
#     self._log.debug("Performing handshake...")
#
#     try:
#         self._log.debug("Sending handshake message...")
#         await self._send_handshake()
#         self._log.debug("Waiting for handshake response")
#         await asyncio.wait_for(self._is_connected.wait(), timeout_seconds)
#         self._log.info("API connection ready, server version 176")
#
#     except asyncio.TimeoutError as e:
#         self._log.error(f"Handshake failed {e!r}")
#
# async def _send_handshake(self) -> None:
#     msg = b"v%d..%d" % (self._min_version, self._max_version)
#     msg = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
#     msg = b"API\x00" + msg
#     self._sendMsg(msg)
#
# def _process_handshake(self, msg: bytes):
#     self._log.debug(f"Processing handshake message {msg}")
#
#     msg = msg.decode(errors="backslashreplace")
#     fields = msg.split("\0")
#     fields.pop()
#
#     id = int(fields[0])
#     self._handshake_message_ids.append(id)
#     # self._log.debug(str(self._handshake_message_ids))
#     # self._log.debug(str(all(id in self._handshake_message_ids for id in (176, 15, 9))))
#
#     if self._handshake_message_ids == [176] and len(fields) == 2:
#         version, _ = fields
#         assert int(version) == 176
#         self._log.debug("Sending startApi message...")
#         # <Start Api Req Code><Version><ClientId> -> struct.pack
#         msg = b"71\x002\x00" + str(self.client_id).encode() + b"\x00\x00"
#         msg = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
#         self._sendMsg(msg)
#     elif all(id in self._handshake_message_ids for id in (176, 15)):
#         # reconnect subscriptions
#         if len(self._subscriptions) > 0:
#             self._log.debug(f"Reconnecting subscriptions {self._subscriptions=}")
#             for sub in self._subscriptions.values():
#                 sub.subscribe()
