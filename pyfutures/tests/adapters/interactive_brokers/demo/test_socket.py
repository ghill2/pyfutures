
import pytest


class TestSocket:
    @pytest.mark.asyncio()
    async def test_connect(self, socket):
        await socket.connect()
