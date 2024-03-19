import asyncio
import queue
import struct
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
from asyncio.protocols import Protocol

# transport -> _SelectorSocketTransport > asyncio/selector_events
from typing import Any

from ibapi import comm
from ibapi.decoder import Decoder

from pyfutures.client.objects import ClientException
from pyfutures.client.objects import ClientRequest
from pyfutures.logger import LoggerAdapter
# from pyfutures.client.client import InteractiveBrokersClient
#


class MyProtocol(Protocol):
    def connection_made(self, transport):
        print("connection made")

    def data_received(self, data):
        print("data received")

    def eof_received(self):
        print("eof received")

    def connection_lost(self, exc):
        print("connection lost")


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

    View socket connections:
        lsof -i :<port_number>
    Analyze buffer bytestrings sent and received on the socket:
        sudo tcpdump -i lo0 -n port 4002 -X

    To have the Decoder in the connection and not the client with separate logging:

    - If using Connection as an inherited base class (InteractiveBrokersClient(Connection))
      -- Access to client for Decoder and subscriptions without additional configuration
      -- Separate connection logger
      -- ibapi calls client.conn.sendMsg() -> this needs a wrapper and feels hacky...

    - If using Connection as client.conn = Connection()
      -- connection needs a reference to the client to pass to the decoder, eg Connection(wrapper=self) in the client is necessary
      -- self._subscriptions can optionally be handled in the client by registering a callback to the Connection.is_connected event
      -- or subscriptions can be pass down from the client
      -- no wrapper class needed client.conn.sendMsg()

    - If using Connection as a MixIn:
      -- Decoder has access to client and Connection has access to subscriptions
      -- connection needs its own logger ? ...
      -- client.conn.sendMsg() needs a wrapper...

    """

    def __init__(self, loop, decoder_wrapper):
        # Connect
        self._loop = loop
        self._connect_task: asyncio.Task | None = None
        self._is_connected = asyncio.Event()
        self._is_connecting_lock = asyncio.Lock()

        # Reader
        self._read_task: asyncio.Task | None = None
        self._read_lock = asyncio.Lock()
        self._decoder = Decoder(serverVersion=176, wrapper=decoder_wrapper)

        # Writer
        self.write_task: asyncio.Task | None = None
        self._write_queue = queue.Queue()

        self._log = LoggerAdapter.from_name(name=type(self).__name__)

        self._request_id_seq = -10  # reset on every connect

        self._requests = {}
        self._subscriptions = {}
        self._executions = {}  # hot cache

    ################################################################################################
    # Read
    #
    async def read(self, buf):
        """
        called from the _read_task() continuously after handshake / is_connected
        and from the connect() for the handshake
        """
        pass
        # while True:
        #     # data = await self._reader.readuntil(b"\x00\x00\x00\x00")
        #     await self._reader._wait_for_data("read")
        #     self._log.debug(f"<-- {self._reader._buffer}")
        #     await asyncio.sleep(0)

    # async def read(self, buf):
    #     """
    #     called from the _read_task() continuously after handshake / is_connected
    #     and from the connect() for the handshake
    #     """
    #     try:
    #         data = await self._reader.readuntil(b"\x00\x00\x00\x00")
    #         self._log.debug(f"<-- {data}")
    #     except ConnectionResetError as e:
    #         self._log.error("ConnectionResetError: Clearing _is_connected immediately, reconnecting soon...")
    #         self._is_connected.clear()
    #         return
    #
    #     if len(data) == 0:
    #         self._log.error("Empty Bytestring Received: Clearing _is_connected immediately, reconnecting soon...")
    #         self._is_connected.clear()
    #         raise Exception("")
    #
    #     buf += data
    #
    #     while len(buf) > 0:
    #         (size, msg, buf) = comm.read_msg(buf)
    #         self._log.debug(f"<-- {size}: {msg}")
    #
    #         if msg:
    #             fields = comm.read_fields(msg)
    #             await asyncio.sleep(0)
    #             return fields
    #         else:
    #             self._log.debug("more incoming packet(s) are needed ")
    #             break
    #
    # await asyncio.sleep(0)

    # async def read(self):
    #     """
    #     called from the _read_task() continuously after handshake / is_connected
    #     and from the connect() for the handshake
    #     """
    #
    #     while True:
    #         buf = b""
    #         try:
    #             print("WAITING AT READER READ")
    #             data = await self._reader.read(4096)
    #             print("DATA")
    #             print(data)
    #         except ConnectionResetError as e:
    #             self._log.error("ConnectionResetError: Clearing _is_connected immediately, reconnecting soon...")
    #             self._is_connected.clear()
    #             raise
    #
    #         if len(data) == 0:
    #             self._log.error("Empty Bytestring Received: Clearing _is_connected immediately, reconnecting soon...")
    #             self._is_connected.clear()
    #             raise Exception("")
    #
    #         buf += data
    #         (size, msg, buf) = comm.read_msg(buf)
    #         print("msg")
    #         print(msg)
    #         fields = comm.read_fields(msg)
    #         self._log.debug(f"<-- {fields}")
    #         return fields
    #
    async def read_task(self):
        """
        starts before initial connect
        """
        while True:
            try:
                print("READ TASK READ")
                buf = b""
                fields = await self.read(buf=buf)
                self._decoder.interpret(fields)
                await asyncio.sleep(0)
            except Exception as e:
                self._log.exception("_read task exception", e)

    ################################################################################################
    # Connect
    # @staticmethod
    async def open_connection(self, host, port):
        """
        So the connection can be mocked more easily
        """
        # asyncio.open_connection()
        # self._reader = StreamReader(loop=self._loop)
        self._protocol = MyProtocol()
        self._transport, _ = await self._loop.create_connection(lambda: self._protocol, host, port)
        # self._writer = StreamWriter(self._transport, self._protocol, self._reader, self._loop)
        return self._protocol, self._transport
        # return await asyncio.open_connection(host, port)

    async def connect(
        self,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
        timeout_seconds: int = 5,
    ):
        """Connect for the first time"""
        if self._is_connected.is_set():
            return

        self._reader, self._writer = await self.open_connection(host, port)
        print("open connection")
        print(self._reader)
        print(self._writer)
        # self._writer = StreamWriter(self._transport, self._protocol, self._reader, self._loop)

        await self.reconnect(client_id=client_id)

        self._write_task = self._loop.create_task(
            coro=self._write_task(),
            name="_write_task",
        )
        # await self._connect_start_tasks(client_id, timeout_seconds)

    async def _connect_start_tasks(self, client_id, timeout_seconds):
        """
        extension of connect
        separated into a different function to mock
        """
        self._connect_task = self._loop.create_task(self.connect_task(client_id, timeout_seconds), name="connect")
        self._read_task = self._loop.create_task(self.read_task(), name="read")

    async def reconnect(self, client_id):
        await self._is_connecting_lock.acquire()

        self._request_id_seq = -10

        # self._log.info("===========================")
        self._log.debug("Connecting...")

        # send handshake request
        self._log.debug("- Sending handshake request...")
        _min_version = 176
        _max_version = 176
        msg = b"v%d..%d" % (_min_version, _max_version)
        handshake_req = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
        handshake_req = b"API\x00" + handshake_req
        self._write(handshake_req)

        # read and process handshake response
        self._log.debug("- Waiting for handshake response")
        buf = b""
        fields = await self.read(buf=buf)
        id = int(fields[0])

        if id != 176:
            return

        # send startApi request
        self._log.debug("- Sending StartAPI request...")
        msg = b"71\x002\x00" + str(client_id).encode() + b"\x00\x00"
        startapi_req = struct.pack("!I%ds" % len(msg), len(msg), msg)  # comm.make_msg
        self._write(startapi_req)
        self._log.debug("- Waiting for StartAPI response...")
        buf = b""
        fields = await self.read(buf=buf)
        id = int(fields[0])

        if id != 15:
            return

        # reconnect subscriptions
        if len(self._subscriptions) > 0:
            self._log.debug(f"- Reconnecting subscriptions {self._subscriptions=}")
            for sub in self._subscriptions.values():
                sub.subscribe()

        self._is_connecting_lock.release()

        self._is_connected.set()
        self._log.info("- Connected Successfully...")
        # self._log.info("===========================")

    async def connect_task(self, client_id, timeout_seconds):
        """
        starts when connect() is called
        """
        # await self._is_connecting_lock.acquire()
        while True:
            if self._is_connected.is_set():
                await asyncio.sleep(5)
                continue

            try:
                await asyncio.wait_for(self.reconnect(client_id), timeout=timeout_seconds)
            except Exception as e:
                self._log.exception("_connect task exception", e)
            # else:
            # await self._is_connecting_lock.release()

    ################################################################################################
    # Write - Queue

    async def _write_task(self):
        """
        process outgoing msg queue
        if the client is connected, send the message, if it is not connected, do nothing
        """
        print("write_task")
        print(self._write_queue)
        while not self._write_queue.empty():
            if self._is_connected.is_set():
                msg: bytes = self._write_queue.get()
                self._write(msg)
            else:
                self._log.warning("Message tried to send when the client is disconnected. Waiting until the client is reconnected...")
                await self._is_connected.wait()

    def _write(self, msg: bytes):
        """
        write, bypass the queue (for handshake)
        """
        # self._log.debug(f"--> generated: {repr(msg)}")
        # msg = b"API\x00\x00\x00\x00\tv176..176"
        self._log.debug(f"--> {repr(msg)}")
        self._writer.write(msg)
        # self._loop.create_task(self._writer.drain())

    ################################################################################################
    # Write - Requests
    #
    def sendMsg(self, msg: bytes) -> None:
        """
        messages output from self.eclient are sent here
        """
        print(self._writer)
        self._write_queue.put(msg)

    def _next_request_id(self) -> int:
        current = self._request_id_seq
        self._request_id_seq -= 1
        return current

    def _create_request(
        self,
        id: int,
        timeout_seconds: int,
        data: list | dict | None = None,
    ) -> ClientRequest:
        assert isinstance(id, int)
        request = ClientRequest(id=id, data=data, timeout_seconds=timeout_seconds)

        self._requests[id] = request

        return request

    async def _wait_for_request(self, request: ClientRequest) -> Any:
        try:
            await asyncio.wait_for(request, timeout=request.timeout_seconds)
        except asyncio.TimeoutError:
            del self._requests[request.id]
            raise
        print("WAITING")
        result = request.result()

        del self._requests[request.id]

        if isinstance(result, ClientException):
            self._log.error(result.message)
            raise result

        return result
