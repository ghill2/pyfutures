import asyncio
import logging
from collections import deque
from unittest.mock import AsyncMock
from unittest.mock import MagicMock


from contextlib import asynccontextmanager


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

        self._responses = []
        self._to_send = []

    async def _handle_read(self, _):
        """
        do not yield, otherwise the function is not executed
        """
        print("_handle_read")
        while len(self._to_send) == 0:
            print(self._to_send)
            await asyncio.sleep(0.1)
        print("_after read")
        print("responding self._to_send", self._to_send)
        msg = self._to_send.pop(0)
        print(f"responding: {msg}")
        if isinstance(msg, bytes):
            return msg
        else:
            raise msg

    def _handle_write(self, msg: bytes) -> None:
        """
        Executed every time Connection._writer.write() is executed
        Ensures a transaction Client -> Gateway, Gateway -> Server
        do not yield, otherwise the function is not executed
        """
        print(f"_handle_write: {msg}")

        if msg == b"API\x00\x00\x00\x00\tv176..176":
            res = [
                b"\x00\x00\x00*176\x0020240308 13:30:34 Greenwich Mean Time\x00",
            ]
            self._to_send.extend(res)
        elif msg.startswith(b"\x00\x00\x00") and b"71\x002\x00" in msg:
            res = [
                b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00",
                # b"\x00\x00\x00\x069\x001\x006\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00",
            ]

            self._to_send.extend(res)
        else:
            req_res = self._responses.pop(0)
            assert msg == req_res["req"]
            print("_handle_write: adding to self._to_send: ", req_res["res"])
            self._to_send.extend(req_res["res"])

            # self.respond.set()

    def queue_response(self, req, res):
        """
        Queue bytes to respond with when a request is received at writer.write()
        """
        self._responses.append(dict(req=req, res=res if isinstance(res, list) else [res]))
        print("queued respons", self._responses)

    def send_response(self, msg):
        """
        Send bytes immediately to the _listen task / reader.read()
        """
        self._to_send.append(msg)
        # self.respond.set()

    def disconnect(self):
        self.send_response(b"")
