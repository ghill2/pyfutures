
import pytest


class TestSocket:
    @pytest.mark.asyncio()
    async def test_perform(self, handshake):
        await handshake.perform()
