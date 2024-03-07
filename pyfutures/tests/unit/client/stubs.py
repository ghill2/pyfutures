import tempfile
from pathlib import Path
import asyncio
import logging
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.historic import InteractiveBrokersHistoric
from pyfutures.client.connection import Connection

class ClientStubs:
    
    @staticmethod
    def client() -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=asyncio.get_event_loop(),
            host="127.0.0.1",
            port=4002,
            log_level=logging.DEBUG,
            api_log_level=logging.DEBUG,
            request_timeout_seconds=0.5,  # requests should fail immediately for unit tests
            override_timeout=True,  # use timeout for all requests even if timeout is given
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