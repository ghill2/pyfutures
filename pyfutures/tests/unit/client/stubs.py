import tempfile
from pathlib import Path
import asyncio
import logging
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.historic import InteractiveBrokersHistoric
from pyfutures.client.connection import Connection

class ClientStubs:
    
    @staticmethod
    def client(
        request_timeout_seconds: float = 0.5,   # requests should fail immediately for unit tests
        override_timeout: bool = True,  # use timeout for all requests even if timeout is given
    ) -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=asyncio.get_event_loop(),
            host="127.0.0.1",
            port=4002,
            log_level=logging.DEBUG,
            api_log_level=logging.DEBUG,
            request_timeout_seconds=request_timeout_seconds,
            override_timeout=override_timeout,
        )
    
    @staticmethod
    def connection(client_id: int) -> Connection:
        return Connection(
            loop=asyncio.get_event_loop(),
            host="127.0.0.1",
            port=4002,
            client_id=client_id,
            
        )
    @classmethod
    def historic(cls) -> InteractiveBrokersHistoric:
        return InteractiveBrokersHistoric(
            client=cls.client(),
            cachedir=Path(tempfile.gettempdir()),
        )