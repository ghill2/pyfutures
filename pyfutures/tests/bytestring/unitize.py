import asyncio
import inspect
from pyfutures.logger import LoggerAdapter
import sys
from pyfutures import PACKAGE_ROOT
import subprocess
import socket

# async parts of unit I need to run:
# poll for
#
# WRITE TO STDIN OF SUBPROCESS TO LOAD BYTESTRINGS
# READ FROM STDOUT OF SUBPROCESS. stdout can send EXIT, this will quit the parent process
# # delete bytestringcommclient, only keep mockserver subproc
#
# creating a thread will still block the event loop, and I cant


# class BytestringCommClient:
#     """
#     Either the subprocess or the comm client has to be asynchronous
#     to monitor if the server wants to stop the tests
#     """
#
#     host = "127.0.0.1"
#     port = 9999
#
#     async def __init__(self):
#         # TODO: dont call from init when I change caching
#         self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
#         self._log = LoggerAdapter.from_attrs(name=type(self).__name__)
#
#     # Using subprocess async poll to exit parent process from child process for tests
#     # async def _read_task(self):
#     #     try:
#     #         # Read data from the server asynchronously
#     #         while True:
#     #             data = await self._reader.read(1024)  # Adjust buffer size as needed
#     #             if not data:  # Check for EOF (empty data)
#     #                 self._log.debug("Server disconnected. Exiting...")
#     #                 raise asyncio.CancelledError  # Raise an exception for test handling
#     #
#     #             # Process received data here (replace with your logic)
#     #             self._log.debug(f"Received data: {data.decode()}")
#     #             await asyncio.sleep(0)
#     #        except asyncio.CancelledError:
#     # pass  # Handle disconnection gracefully (optional)
#     # except Exception as e:
#     # self._log.error(f"Error during communication: {e}")
#     # raise  # Re-raise exception for potential test failure
#     #
#
# async def load_bytestrings(self, bytestrings_path: str):
#     """Connect + load_bytestrings"""
#     await self._writer.write(fn_hash_bytes)
#     fn_hash_bytes = str(bytestrings_path).encode()
#     self._log.debug(f"Sent bytestring path to mock server: {bytestrings_path}")
# #
# async def close(self):
#     await self._writer.drain()  # Wait for data to be flushed
#     self._writer.close()
#     await self._writer.wait_closed()  # Wait for the socket to close
#
#
# class BytestringCommClient:
#     host = "127.0.01"
#     port = 9999
#
#     def __init__(self):
#         self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self._log = LoggerAdapter.from_attrs(name=type(self).__name__)
#
#     def load_bytestrings(self, fn_hash: int):
#         self._sock.connect((self.host, self.port))
#         self._log.debug(f"Connected to server at {self.host}:{self.port}")
#         fn_hash_bytes = str(fn_hash).decode()
#         self.writer.write(fn_hash_bytes)
#
#     async def close(self):
#         self._sock.close()
#

#
# class BytestringCommClient:
#     """
#     The mock_server runs in a separate process when the tests start and creates a comm socket
#     this client sends the hash to the comm socket of the mock_server
#     so the mock server can load the bytestrings for the test
#     """
#
#     def __init__(self):
#         self._log = LoggerAdapter.from_attrs(name=type(self).__name__)
#
#     async def connect(self):
#         self.reader, self.writer = await asyncio.open_connection("127.0.0.1", 9999)
#
#     # async def load_bytestrings(self, hash):
#     #     """
#     #     Sends the has of the function
#     #     """
#     #     print(hash)
#     #     print(len(hash))
#     #     self.send("")
#
#     async def load_bytestrings(self, fn_hash: int):
#         self.writer.write(str(fn_hash).decode())  # Encode message to bytes
#
#     async def close(self):
#         self.writer.close()
#         await self.writer.wait_closed()
