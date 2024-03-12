import pickle
import asyncio
from collections.abc import Callable

from pathlib import Path
from typing import Any

import pandas as pd
from ibapi.contract import Contract as IBContract

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import WhatToShow
from pyfutures.adapter.parsing import AdapterParser  # TODO: change to just tradingclass
from pyfutures.client.objects import ClientException
from pyfutures.client.parsing import ClientParser
from pyfutures.logger import LoggerAdapter


class Cache:
    def __init__(self, path: Path):
        self.path = Path(path)  # directory
        self._parser = ClientParser()
        self._log = LoggerAdapter.from_name(name=type(self).__name__)

    def get(
        self,
        key: str,
    ) -> list[Any] | Exception | None:
        
        
        pickle_path = self._pickle_path(key)
        parquet_path = self._parquet_path(key)
        if pickle_path.exists() and parquet_path.exists():
            raise RuntimeError("Invalid cache")
        elif not pickle_path.exists() and not parquet_path.exists():
            return None
        elif parquet_path.exists():
            cached = pd.read_parquet(parquet_path)
            cached = self._parser.bar_data_from_dataframe(cached)
        elif pickle_path.exists():
            cached = self._read_pickle(pickle_path)
        return cached

    def set(
        self,
        key: str,
        value: list[Any] | Exception,
    ) -> None:
        if not isinstance(value, (list, Exception)):
            raise RuntimeError(f"Unsupported type {type(value).__name__}")

        if isinstance(value, list):
            df = self._parser.bar_data_to_dataframe(value)
            df.to_parquet(self._parquet_path(key), index=False)
            return
        elif isinstance(value, ClientException):
            value = value.to_dict()

        with open(self._pickle_path(key), "wb") as f:
            pickle.dump(value, f)

    def purge_errors(self, cls: type | tuple[type] = Exception) -> None:
        for path in self.path.glob("*.pkl"):
            cached = self._read_pickle(path)
            if isinstance(cached, cls):
                path.unlink()
    
    @staticmethod
    def _read_pickle(path: Path) -> Exception:
        with open(path, "rb") as f:
            cached = pickle.load(f)
            if isinstance(cached, dict):
                cached = ClientException.from_dict(cached)
        return cached
        
    def _parquet_path(self, key: str) -> Path:
        return self.path / f"{key}.parquet"
    
    def _pickle_path(self, key: str) -> Path:
        return self.path / f"{key}.pkl"

    def __len__(self) -> int:
        return len(list(self.path.rglob("*.pkl")))


class CachedFunc(Cache):
    """
    Creates a cache
    name: str -> the subdirectory of the cache, eg request_bars, request_quote_ticks, request_trade_ticks
    """

    def __init__(self, func: Callable, cache_dir: Path):
        super().__init__(cache_dir)

        self._func = func
        self._log = LoggerAdapter.from_name(name=type(self).__name__)

    async def __call__(self, *args, **kwargs) -> list[Any] | Exception:
        assert args == (), "Keywords arguments only"

        self.path.mkdir(parents=True, exist_ok=True)

        key = self.build_key(*args, **kwargs)

        cached = self.get(key)
        if cached is not None:
            self._log.debug(f"Returning cached {key}={self._value_to_str(cached)}")
            if isinstance(cached, Exception):
                raise cached
            else:
                return cached

        self._log.debug(f"No cached {key}")

        try:
            result = await self._func(**kwargs)
            self.set(key, result)
            self._log.debug(f"Saved {self._value_to_str(result)} items...")
            return result
        except Exception as e:
            self._log.error(str(e))
            self.set(key, e)
            self._log.debug(f"Saved {e} items...")
            raise

    @classmethod
    def build_key(cls, **kwargs):
        parsing = {
            # TODO: change to just trading class
            IBContract: lambda x: f"{x.tradingClass}-{x.exchange}-{x.secType}",
            pd.Timestamp: lambda x: x.strftime("%Y-%m-%d-%H-%M-%S"),
            Duration: lambda x: x.value,
            BarSize: lambda x: str(x).replace(" ", "-"),
            WhatToShow: lambda x: x.value,
        }

        parts = []
        for x in kwargs.values():
            parsing_func = parsing.get(type(x))

            if parsing_func is None:
                raise RuntimeError(f"Unable to build key which argument type {type(x).__name__}, define a parsing method.")

            part: str = parsing_func(x)
            assert isinstance(part, str), f"Check parsing func for type {type(x).__name__} return type str"
            parts.append(part)

        key = "=".join(parts)

        return cls._sanitize_filename(key)

    def is_cached(self, *args, **kwargs) -> bool:
        assert args == (), "Keywords arguments only"
        key = self.build_key(**kwargs)
        cached = self.get(key)
        return cached is not None

    @staticmethod
    def _value_to_str(value: Exception | list) -> str:
        if isinstance(value, Exception):
            return repr(value)
        elif isinstance(value, list):
            return f"{len(value)} items"

        raise NotImplementedError

    @staticmethod
    def _sanitize_filename(filename):
        """
        Sanitize a string value for safe storage in a file name
        across Windows, Linux, and macOS operating systems.
        """
        illegal_chars = ["<", ">", ":", '"', "/", "\\", "|", "?", "*"]
        sanitized_filename = filename

        # Replace illegal characters with underscore (_)
        for char in illegal_chars:
            sanitized_filename = sanitized_filename.replace(char, "_")

        # Remove leading and trailing whitespaces and dots
        sanitized_filename = sanitized_filename.strip().strip(".")

        return sanitized_filename


# class RequestBarsCachedFunc(CachedFunc):
#     def __init__(
#         self,
#         client: InteractiveBrokersClient,
#         name: str,
#         timeout_seconds: int,
#     ):
#         self._client = client
#         self._timeout_seconds = timeout_seconds
#         self._log = Logger("RequestsCache")
#         super().__init__(name)


# def in_cache(self, *args, **kwargs):
#     key = self._build_key(*args, **kwargs)
#     pkl_path = self.cachedir / f"{key}.pkl"
#     json_path = self.cachedir / f"{key}.json"
#     return pkl_path.exists() or json_path.exists()

# def write_error(
#     self,
#     key: str,
#     value: Exception,
# ) -> None:
#     self._log.debug(
#         f"set_errors: storing error response in cache: {value=}", LogColor.BLUE
#     )
# self._log.debug(f"set_data: storing data in cache: {len(value)}", LogColor.BLUE)

# self._log.debug(
#     f"get data: cached data exists: returning: {key}", LogColor.BLUE
# )
# self._log.debug(
#     f"get_errors: cached error response exists: {cached_error=} {key}",
#     LogColor.BLUE,
# )

# # Create a color formatter with blue for INFO messages
# formatter = colorlog.ColoredFormatter(
#     "%(log_color)s%(levelname)s:%(name)s:%(message)s %(reset)s",
#     log_colors={
#         'DEBUG': 'blue',
#         'INFO': 'blue',  # Set blue for INFO messages
#         'WARNING': 'blue',
#         'ERROR': 'blue',
#         'CRITICAL': 'blue,blue'
#     }
# )

# # Create a stream handler and set the formatter
# handler = logging.StreamHandler()
# handler.setFormatter(formatter)
