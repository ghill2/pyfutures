from unittest.mock import AsyncMock
from unittest.mock import MagicMock
import asyncio
from collections import deque
import logging
from unittest.mock import patch


class MockSocket:

    def __init__(self):
        # append to this queue to receive msg at self._reader.read()
        self._mocked_responses = deque()

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = self._handle_read

        self.respond = asyncio.Event()

        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_writer.write.side_effect = self._handle_write

        self.mock_reader = mock_reader
        self.mock_writer = mock_writer

        self._log = logging.getLogger("MockSocket")
        
        self._responses = {
            b"API\x00\x00\x00\x00\nv176..176 ": [
                b'\x00\x00\x00*176\x0020240307 13:56:35 Greenwich Mean Time\x00',
            ],
            b"\x00\x00\x00\x0871\x002\x001\x00\x00": [
                b'\x00\x00\x00\x0f15\x001\x00DU7855823\x00',
                b"\x00\x00\x00\x089\x001\x00530\x00\x00\x00\x0064\x002\x00-1\x002104\x00Market data farm connection is OK:usfarm\x00\x00\x00\x00\x0044\x002\x00-1\x002106\x00HMDS data farm connection is OK:ushmds\x00\x00",
                b'\x00\x00\x00\x069\x001\x006\x00',
            ]
        }
        self._to_send = deque()
        
    async def _handle_read(self, _):
        """
        do not yield, otherwise the function is not executed
        """
        self._log.debug("_read_generator")
        
        while len(self._to_send) == 0:
            await asyncio.sleep(0)
            
        msg: bytes = self._to_send.popleft()
        print(f"responsed: {msg}")
        return msg
            
    def _handle_write(self, msg: bytes) -> None:
        """
        Executed every time Connection._writer.write() is executed
        Ensures a transaction Client -> Gateway, Gateway -> Server
        do not yield, otherwise the function is not executed
        """
        print(f"write: {msg}")
        responses = self._responses.get(msg)
        print(len(responses))
        if responses is not None:
            self._to_send.extend(responses)
            
    async def disconnect(self):
        # mock a disconnect by sending send an empty byte string to the client
        self._to_send.append(b"")

