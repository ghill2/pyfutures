import pandas as pd
import tempfile
from pathlib import Path
import asyncio
import logging
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.historic import InteractiveBrokersHistoric
from pyfutures.client.connection import Connection
from pyfutures.logger import LoggerAdapter

class ClientStubs:
    
    @staticmethod
    def logger_adapter(
        level: int = logging.DEBUG,
        path: Path | None = None
    ) -> LoggerAdapter:

        logger =  LoggerAdapter(
            name="TestModule",
            id="identifier",
            level=level,
            path=path,
        )
        logger.set_timestamp_ns(
            pd.Timestamp("2023-01-01 08:12:49.19827", tz="UTC").value
        )
        return logger
        
        
    @staticmethod
    def client(
        request_timeout_seconds: float = 0.5,   # requests should fail immediately for unit tests
        override_timeout: bool = True,  # use timeout for all requests even if timeout is given
        api_log_level: int = logging.DEBUG,
    ) -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=asyncio.get_event_loop(),
            host="127.0.0.1",
            port=4002,
            api_log_level=api_log_level,
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
            use_cache=True,
            cache_dir=Path(tempfile.gettempdir()),
        )