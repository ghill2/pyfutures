from unittest.mock import AsyncMock
from unittest.mock import MagicMock
import asyncio
from collections import deque
import logging
from unittest.mock import patch


class MockSocket:
    """ """

    def __init__(self):
        # append to this queue to receive msg at self._reader.read()
        self._mocked_responses = deque()

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = self._read_generator

        self.respond = asyncio.Event()

        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_writer.write.side_effect = self._write_generator

        # internal storage for req res
        self._requests = deque()
        self._responses = deque()

        self.mock_reader = mock_reader
        self.mock_writer = mock_writer

        self._log = logging.getLogger("MockSocket")
        
    async def _read_generator(self, _):
        """
        do not yield, otherwise the function is not executed
        """
        self._log.debug("_read_generator")
        if len(self._mocked_responses) == 0:
            self.respond.clear()

        await self.respond.wait()

        response = self._mocked_responses.popleft()
        self._log.debug(f"_read_generator: sending {response}")
        return response

    def _write_generator(self, msg):
        """
        Executed every time Connection._writer.write() is executed
        Ensures a transaction Client -> Gateway, Gateway -> Server
        do not yield, otherwise the function is not executed
        """
        self._log.debug(f"_write_generator: {msg}")
        assert (
            msg == self._requests.popleft()
        ), "expected write value != actual write value"
        responses = self._responses.popleft()
        self._mocked_responses.extend(responses)
        self.respond.set()

    def send_responses(self, responses):
        """
        send a response from gateway to client immediately without an associated client request
        eg, for simulating empty bytestrings
        """
        assert isinstance(responses, list)
        self._mocked_responses.extend(responses)

    def queue_transaction(self, request, responses):
        """

        Queue a response that will be received at a later time
        Example:
          - For the request, send the responses when the client executed _listen socket.read()

        """
        assert isinstance(request, bytes)
        assert isinstance(responses, list)
        self._requests.append(request)
        self._responses.append(responses)

    ############## HIGH LEVEL METHODS ###############

    def queue_handshake(self):
        """
        High Level, Queues the handshake routine
        """
        # handshake message
        self.queue_transaction(
            request=b"API\x00\x00\x00\x00\nv176..176 ",
            # res=[b'176\x0020240229 12:41:55 Greenwich Mean Time\x00'],
            # responses=[b'176\x0020240303 03:56:49 GMT\x00']
            responses=[
                b"\x00\x00\x00*176\x0020240303 20:39:51 Greenwich Mean Time\x00"
            ],
        )

        # startApi message
        self.queue_transaction(
            request=b"\x00\x00\x00\x0871\x002\x001\x00\x00",
            # responses=[b"15\x001\x00DU1234567\x00", b"9\x001\x006\x00"],
            responses=[
                b"\x00\x00\x00\x0f15\x001\x00DU7606863\x00",
                b"\x00\x00\x00\x089\x001\x00530\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00\x00\x00\x0044\x002\x00-1\x002106\x00HMDS data farm connection is OK:ushmds\x00\x00"
                b"9\x001\x00530\x00",
            ],
        )

    # async def connect(self, connection):


    async def disconnect(self, connection):
        self.send_responses([b""])
        # await asyncio.wait_for(lambda: not connection._is_connected(), timeout=10)
        # wait until unset
        while connection._is_connected.is_set():
            await asyncio.sleep(0.1)

