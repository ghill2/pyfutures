import asyncio
import logging
from collections import deque
from unittest.mock import AsyncMock
from unittest.mock import MagicMock


class MockServer:
    def __init__(self):
        # append to this queue to receive msg at self._reader.read()
        self._mocked_responses = deque()

        self.reader = AsyncMock()
        self.reader.read.side_effect = self._handle_read

        self.respond = asyncio.Event()

        self.writer = MagicMock()
        self.writer.drain = AsyncMock()
        self.writer.write.side_effect = self._handle_write

        self._log = logging.getLogger("MockSocket")
        # self._log.setLevel(logging.DEBUG)

        self._to_send = deque()

    async def _handle_read(self, _):
        """
        do not yield, otherwise the function is not executed
        """

        if len(self._to_send) == 0:
            self.respond.clear()
        await self.respond.wait()

        msg = self._to_send.popleft()
        if isinstance(msg, bytes):
            return msg
        else:
            raise msg


        print(f"responsed: {msg}")


    def _handle_write(self, msg: bytes) -> None:
        """
        Executed every time Connection._writer.write() is executed
        Ensures a transaction Client -> Gateway, Gateway -> Server
        do not yield, otherwise the function is not executed
        """
        print(f"write: {msg}")

        responses = []
        if msg == b"API\x00\x00\x00\x00\nv176..176 ":
            responses = [
                b"\x00\x00\x00*176\x0020240308 13:30:34 Greenwich Mean Time\x00",
            ]
        elif msg.startswith(b"\x00\x00\x00\t71\x002\x00"):
            responses = [
                b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00",
                b"\x00\x00\x00\x069\x001\x006\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00",
            ]

        self._to_send.extend(responses)
        self.respond.set()


    def send_response(self, msg):
        # mock a disconnect by sending send an empty byte string to the client
        self._to_send.append(msg)
        self.respond.set()
        # process the reader task now
        # incase this is called at the end of a test
        # await asyncio.sleep(0)

    def disconnect(self):
        self.send_response(b"")
