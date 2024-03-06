import logging
import asyncio
import os
import struct
from collections.abc import Coroutine
import psutil
from ibapi import comm

# this bug is still present on the gnznz fork:
# https://github.com/UnusualAlpha/ib-gateway-docker/issues/88


class Connection:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        host: str,
        port: int,
        log_level: int = logging.WARN,
    ):
        self._loop = loop
        self._host = host
        self._port = port

        self._is_connected = asyncio.Event()
        self._is_connecting_lock = asyncio.Lock()
        self._watch_dog_task: asyncio.Task | None = None
        self._listen_task: asyncio.Task | None = None

        self._log = logging.getLogger(self.__class__.__name__)
        # FIX THIS - log_level does not work with pytest tests
        self._log.setLevel(level=log_level)

        self._handlers = set()

        self._handshake_message_ids = []
        self._reader, self._writer = (None, None)

    def register_handler(self, handler: Coroutine) -> None:
        self._handlers.add(handler)

    async def _listen(self) -> None:
        assert self._reader is not None

        buf = b""

        while True:
            try:
                data = await self._reader.read(4096)
                # self._log.debug(f"<-data: {data}")
            except ConnectionResetError as e:
                self._log.error(f"listen: TWS closed the connection {e!r}...")
                self._is_connected.clear()
                return

            if data == b"":
                self._log.debug(
                    "0 bytes received from server, connect has been dropped"
                )
                self._is_connected.clear()
                return

            buf += data

            while len(buf) > 0:
                (size, msg, buf) = comm.read_msg(buf)

                if msg:
                    self._log.debug(f"<-- {msg!r}")

                    self._handle_msg(msg)
                    await asyncio.sleep(0)

                else:
                    self._log.debug("more incoming packet(s) are needed ")
                    break

            await asyncio.sleep(0)

    def _handle_msg(self, msg: bytes) -> None:
        if self.is_connected:
            for handler in self._handlers:
                handler(msg)
        else:
            self._process_handshake(msg)

    async def _run_watch_dog(self):
        """
        Monitors the socket connection for disconnections.
        """
        try:
            while True:
                await asyncio.sleep(5)

                if self.is_connected:
                    continue

                self._log.debug(
                    "Watchdog: connection has been disconnected. Reconnecting..."
                )

                await self.connect()

        except Exception as e:
            self._log.error(repr(e))

    async def _initialize(self) -> bool:
        self._log.debug("Initializing...")

        self._handshake_message_ids = []
        self._reader, self._writer = (None, None)

        self._log.debug("Initializing listen task...")
        if self._listen_task is not None:
            self._listen_task.cancel()
        self._listen_task = None

        # close the writer
        try:
            if self._writer is not None and not self._writer.is_closing():
                self._writer.write_eof()
                self._writer.close()
                await self._writer.wait_closed()
        except RuntimeError as e:
            if "unable to perform operation on" in str(e) and "closed=True" in str(e):
                pass
            else:
                raise

        self._log.debug("Successfully Initialized...")

    @property
    def is_connected(self) -> bool:
        return self._is_connected.is_set()

    async def start(self) -> bool:
        self._log.debug("Starting...")

    async def connect(self, timeout_seconds: int = 5) -> None:
        """
        Called by the user
        """
        if self._watch_dog_task is None:
            self._watch_dog_task = self._loop.create_task(self._run_watch_dog())

        async with self._is_connecting_lock:
            await self._connect(timeout_seconds=timeout_seconds)
            await self._handshake(timeout_seconds=timeout_seconds)

    async def _connect(self, timeout_seconds: int = 5) -> None:
        """
        Called by watch_dog and manually with connect() by the user
        NOTE: do not call handshake here so we can test connect and handshake separately
        """
        self._log.debug("Connecting...")

        await self._initialize()

        # connect socket
        self._log.debug("Connecting socket...")
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host, self._port
            )
        except ConnectionRefusedError as e:
            self._log.error(f"Socket connection failed, check TWS is open {e!r}")
            return
        self._log.debug(f"Socket connected. {self._reader} {self._writer}")

        # start listen task
        self._log.debug("Starting listen task...")
        self._listen_task = self._loop.create_task(self._listen(), name="listen")
        self._log.info("Listen task started")

    async def _handshake(self, timeout_seconds: float | int = 5.0) -> None:
        self._log.debug("Performing handshake...")
        try:
            self._log.debug("Sending handshake message...")
            await self._send_handshake()
            self._log.debug("Waiting for handshake response")
            await asyncio.wait_for(self._is_connected.wait(), timeout_seconds)
            self._log.info("API connection ready, server version 176")

        except asyncio.TimeoutError as e:
            self._log.error(f"Handshake failed {e!r}")

    def sendMsg(self, msg: bytes) -> None:
        if not self.is_connected:
            self._log.error("A message was sent when the Connection was disconnected.")
            return
        self._sendMsg(msg)

    def _sendMsg(self, msg: bytes) -> None:
        self._log.debug(f"--> {msg}")
        self._writer.write(msg)
        self._loop.create_task(self._writer.drain())

    async def _handle_disconnect(self):
        """
        Called when the socket has been disconnected for some reason, for example,
        due to a schedule restart or during IB nightly reset.
        """
        self._log.debug("Handling disconnect.")

        # await self.connect()
        # reconnect subscriptions
        # for sub in self._subscriptions:
        #     sub.cancel()

    async def _send_handshake(self) -> None:
        msg = b"API\0" + self._prefix(b"v%d..%d%s" % (176, 176, b" "))
        self._sendMsg(msg)

    def _process_handshake(self, msg: bytes):
        self._log.debug(f"Processing handshake message {msg}")

        msg = msg.decode(errors="backslashreplace")
        fields = msg.split("\0")
        fields.pop()

        id = int(fields[0])
        self._handshake_message_ids.append(id)

        if self._handshake_message_ids == [176] and len(fields) == 2:
            version, _ = fields
            assert int(version) == 176
            self._log.debug("Sending startApi message...")
            self._sendMsg(b"\x00\x00\x00\x0871\x002\x001\x00\x00")
        elif all(id in self._handshake_message_ids for id in (176, 15, 9)):
            self._is_connected.set()

    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack(">I", len(msg)) + msg

    @classmethod
    async def start_tws(cls):
        print("Starting tws...")
        if cls.is_tws_running():
            await cls.kill_tws()
        os.system("sh /opt/ibc/twsstartmacos.sh")

        while not cls.is_tws_running():
            print("Waiting for tws to open...")
            await asyncio.sleep(1)

    @classmethod
    async def kill_tws(cls):
        print("Killing tws...")
        os.system("killall -m java")
        os.system("killall -m Trader Workstation 10.26")
        while cls.is_tws_running():
            print("Waiting for tws to close...")
            await asyncio.sleep(1)

    @staticmethod
    def is_tws_running() -> bool:
        for process in psutil.process_iter(["pid", "name"]):
            name = process.info["name"].lower()
            if name == "java" or name.startswith("Trader Workstation"):
                return True
        return False

    # def error(  # too complex
    #     self,
    #     req_id: int,
    #     error_code: int,
    #     error_string: str,
    #     advanced_order_reject_json: str = "",
    # ) -> None:
    #     self._log.debug(error_string)

    # def is_connected(self) -> bool:
    #     """
    #     Returns False if the socket has been disconnected, for example, due to a schedule restart
    #     or during IB nightly reset.
    #     """
    #     return self._conn.isConnected()

    # return
    # try:
    #     await asyncio.wait_for(self.socket.is_ready.wait(), 5)
    #     return
    # except asyncio.TimeoutError as e:
    #     self._log.error(f"{repr(e)}")

    # # except Exception as e:
    # #     self._log.error(type(e))
    # #     self._log.error(repr(e))
    # #     pass

    # while attempts > 0:

    #     # connect socket
    #     self._log.debug(f"Connecting socket...")
    #     try:
    #         await asyncio.wait_for(self.socket.connect(), 10)
    #         await asyncio.wait_for(self._handshake.perform(), 10)
    #     except (asyncio.TimeoutError, ConnectionRefusedError, ConnectionResetError) as e:
    #         self._log.error(repr(e))
    #         self._log.debug(f"Error during socket connection, reconnecting... attempts={attempts}")
    #         attempts -= 1
    #         await asyncio.sleep(5)
    #         continue

    #     return True

    # self._log.error("Failed to connect")
    # return False
    # # Wait for TWS to start
    # while not self.is_tws_running():
    #     self._log.debug(f"Waiting for tws to start...")
    #     await asyncio.sleep(2.5)
    # connect socket
    # while attempts > 0:

    #     self._log.debug(f"Attempt {attempts}...")
    #     try:

    #         break
    #     except ConnectionRefusedError as e:
    #         """
    #         When TWS is not running, the socket connection will be refused.
    #         """
    #         self._log.error(f"{repr(e)}")
    #         self._log.debug(f"Socket connection refused. Waiting 10 seconds then reattempting...")
    #         attempts -= 1
    #         await asyncio.sleep(10)

    # return

    # # # handshake
    # # while attempts > 0:
    # #     self._log.debug(f"Attempt {attempts}...")
    # #     try:

    # #     except asyncio.TimeoutError as e:
    # #         """
    # #         When TWS is not running, the socket connection will be refused.
    # #         """
    # #         self._log.error(f"{repr(e)}")
    # #         self._log.debug(f"Handshake failed. Waiting 10 seconds then reattempting...")
    # #         attempts -= 1
    # #         await asyncio.sleep(10)

    # self._log.debug("Connection failed")
