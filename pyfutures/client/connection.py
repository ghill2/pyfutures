import asyncio
import queue
import struct
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
from typing import Any

from ibapi import comm
from ibapi.decoder import Decoder

from pyfutures.client.objects import ClientException
from pyfutures.client.objects import ClientRequest
from pyfutures.logger import LoggerAdapter


class Connection:
    """
    Market data connection test
    https://www.interactivebrokers.com/cgi-bin/conn_test.pl

    IB maintenance periods:
    https://www.interactivebrokers.com/en/software/systemStatus.php

    Europe:
        Saturday - Thursday:
            - 05:45 - 06:45 CET
            - 04:45 - 05:45 UTC

        Friday:
            - 23:00 - 03:00 ET
            - 18:00 - 22:00 UTC

    North America:
        Saturday - Thursday:
            - 23:45 - 00:45 ET
            - 18:45 - 19:00 UTC
        Friday:
            - 23:00 - 03:00 ET
            - 18:00 - 22:00 UTC
    """

    def __init__(self, loop):
        self._loop = loop

        # Connect
        self._connect_task: asyncio.Task | None = None
        self._is_connected = asyncio.Event()
        self._is_connecting_lock = asyncio.Lock()

        # Reader
        self._read_task: asyncio.Task | None = None
        self._reader = StreamReader(loop=self._loop)
        self._protocol = StreamReaderProtocol(self._reader, loop=self._loop)
        self._read_lock = asyncio.Lock
        self._decoder = Decoder(serverVersion=176)

        # Writer
        self.write_task: asyncio.Task | None = None
        self._write_queue = queue.Queue()

        self._log = LoggerAdapter.from_name(name=type(self).__name__)

    ################################################################################################
    # Read

    async def read(self):
        """
        called from the _read_task() continuously after handshake / is_connected
        and from the connect() for the handshake
        """
        buf = b""
        while len(buf) < 4096:
            try:
                data = await self._reader.read(4096)
                # self._log.debug(f"<-- {data}")
            except ConnectionResetError as e:
                self._log.debug(f"TWS closed the connection {e!r}...")
                self._is_connected.clear()
                return

            if len(data) == 0:
                self._log.debug("0 bytes received from server, connect has been dropped")
                self._is_connected.clear()
                return

            buf += data
            # await self._queue.put(data)

        # if not msg:
        # self._log.debug(f"error parsing msg")
        #
        (size, msg, buf) = comm.read_msg(buf)
        fields = comm.read_fields(msg)
        return fields

    async def read_task(self):
        """
        starts before initial connect
        """
        while True:
            try:
                await self._read_lock.acquire()
                fields = self.read()
                await self._read_lock.release()
                self._decoder.interpret(fields)
            except Exception as e:
                self._log.exception("_listen task exception", e)

    ################################################################################################
    # Connect

    async def connect(
        self,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
    ):
        """Connect for the first time"""
        if self._is_connected:
            return

        self._transport, _ = await self._loop.create_connection(lambda: self._protocol, host, port)
        self._writer = StreamWriter(self._transport, self._protocol, self._reader, self._loop)

        self._write_task = self._loop.create_task(
            coro=self._write_task(),
            name="_write_task",
        )

        # await asyncio.wait_for(self.reconnect(), timeout=self._request_timeout_seconds)
        self._read_task = self._loop.create_task(self.read_task(), name="read")
        self._connect_task = self._loop.create_task(self.connect_task()(client_id))

    async def reconnect(self, client_id):
        await self._is_connecting_lock.acquire()
        await self._read_lock.acquire()

        # send handshake request
        self._log.debug("Sending handshake request...")
        _min_version = 176
        _max_version = 176
        msg = b"v%d..%d" % (_min_version, _max_version)
        handshake_req = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
        handshake_req = b"API\x00" + msg
        self._writer.write(handshake_req)

        # read and process handshake response
        self._log.debug("Waiting for handshake response")
        id = await self.read()[0]
        # id = int(comm.read_fields(msg)[0])

        if id != 176:
            return

        # send startApi request
        msg = b"71\x002\x00" + str(client_id).encode() + b"\x00\x00"
        startapi_req = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
        self.writer.write(startapi_req)
        id = await self.read()[0]
        # id = int(comm.read_fields(msg)[0])

        if id != 15:
            return

        # reconnect subscriptions
        if len(self._subscriptions) > 0:
            self._log.debug(f"Reconnecting subscriptions {self._subscriptions=}")
            for sub in self._subscriptions.values():
                sub.subscribe()

        await self._is_connecting_lock.release()
        await self._read_lock.release()

        self._is_connected.set()

    async def connect_task(self, client_id):
        """
        starts after initial connect
        """
        while True:
            if self._is_connected.is_set():
                await asyncio.sleep(5)
                continue

            try:
                await asyncio.wait_for(self.reconnect(host, port, client_id), timeout=self._request_timeout_seconds)
            except Exception as e:
                self._log.exception("_read task exception", e)

    ################################################################################################
    # Write - Queue

    async def _write_task(self):
        """
        process outgoing msg queue
        if the client is connected, send the message, if it is not connected, do nothing
        """
        while True:
            if self._is_connected.is_set():
                while not self._write_queue.empty():
                    msg: bytes = self._write_queue.get()
                    self._log.debug(f"--> {msg}")
                    self._writer.write(msg)
                    self.loop.create_task(self._writer.drain())
            else:
                self._log.warn("Message tried to send when the client is disconnected. Waiting until the client is reconnected...")
                await self._is_connected.wait()

    def write(self, msg: bytes):
        """ibapi: client.sendMsg()"""
        self._write_queue.put(msg)

    ################################################################################################
    # Write - Requests
    #
    def sendMsg(self, msg: bytes) -> None:
        """
        messages output from self.eclient are sent here
        """
        self._writer.write(msg)

    def _next_request_id(self) -> int:
        current = self._request_id_seq
        self._request_id_seq -= 1
        return current

    def _create_request(
        self,
        id: int,
        data: list | dict | None = None,
        timeout_seconds: int | None = None,
    ) -> ClientRequest:
        assert isinstance(id, int)

        request = ClientRequest(
            id=id,
            data=data,
            timeout_seconds=timeout_seconds or self._request_timeout_seconds,
        )

        self._requests[id] = request

        return request

    async def _wait_for_request(self, request: ClientRequest) -> Any:
        try:
            await asyncio.wait_for(request, timeout=request.timeout_seconds)
        except asyncio.TimeoutError:
            del self._requests[request.id]
            raise

        result = request.result()

        del self._requests[request.id]

        if isinstance(result, ClientException):
            self._log.error(result.message)
            raise result

        return result
