import asyncio
import logging
import tempfile
from pathlib import Path

import pandas as pd

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.protocol import IBProtocol
from pyfutures.client.historic import InteractiveBrokersHistoricClient
from pyfutures.logger import LoggerAdapter


CLIENT = None


class ClientStubs:
    @staticmethod
    def logger_adapter(level: int = logging.DEBUG, path: Path | None = None) -> LoggerAdapter:
        logger = LoggerAdapter(
            name="TestModule",
            id="identifier",
            level=level,
            path=path,
        )
        logger.set_timestamp_ns(pd.Timestamp("2023-01-01 08:12:49.19827", tz="UTC").value)
        return logger

    @staticmethod
    def client(
        loop: asyncio.AbstractEventLoop = None,
        api_log_level: int = logging.DEBUG,
    ) -> InteractiveBrokersClient:
        global CLIENT
        if CLIENT:
            return CLIENT
        CLIENT = InteractiveBrokersClient(
            loop=loop,
            api_log_level=api_log_level,
        )
        return CLIENT

    @staticmethod
    def uncached_client(
        client_id: int = 1,
        loop: asyncio.AbstractEventLoop = None,
        api_log_level: int = logging.DEBUG,
    ) -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=loop,
            api_log_level=api_log_level,
        )

    # @staticmethod
    # def connection(
    #     loop: asyncio.AbstractEventLoop,  # has to use pytest asyncio event_loop
    #     client_id: int,
    # ) -> Connection:
    #     return Connection(
    #         loop=loop or asyncio.get_event_loop(),
    #     )
    #
    @classmethod
    def historic(cls) -> InteractiveBrokersHistoricClient:
        return InteractiveBrokersHistoricClient(
            client=cls.client(),
            use_cache=True,
            cache_dir=Path(tempfile.gettempdir()),
        )
