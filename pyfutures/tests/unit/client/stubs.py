import asyncio
import logging
import tempfile
from pathlib import Path

import pandas as pd

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.connection import Connection
from pyfutures.client.historic import InteractiveBrokersHistoricClient
from pyfutures.logger import LoggerAdapter


class UnitClientStubs:
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
        request_timeout_seconds: float = 0.5,  # requests should fail immediately for unit tests
        api_log_level: int = logging.DEBUG,
    ) -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=loop or asyncio.get_event_loop(),
            api_log_level=api_log_level,
            request_timeout_seconds=request_timeout_seconds,
        )

    # @staticmethod
    # def connection(
    #     loop=None,
    # ) -> Connection:
    #     return Connection(loop=loop or asyncio.get_running_loop())
    #
    @classmethod
    def historic(cls) -> InteractiveBrokersHistoricClient:
        return InteractiveBrokersHistoricClient(
            client=cls.client(),
            use_cache=True,
            cache_dir=Path(tempfile.gettempdir()),
        )
