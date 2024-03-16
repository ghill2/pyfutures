import pickle
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import WhatToShow
from pyfutures.client.objects import ClientException
from pyfutures.client.parsing import ClientParser
from pyfutures.logger import LoggerAdapter


class BaseCache:
    def __init__(self, path: Path):
        self.path = Path(path)

    def _pickle_path(self, key: str) -> Path:
        return self.path / f"{key}.pkl"

    def __len__(self) -> int:
        return len(list(self.path.rglob("*.pkl")))

    @staticmethod
    def _read_pickle(path: Path) -> Exception:
        with open(path, "rb") as f:
            cached = pickle.load(f)
            if isinstance(cached, dict):
                cached = ClientException.from_dict(cached)
        return cached

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


class RequestsCache(BaseCache):
    def __init__(self, path: Path):
        super().__init__(path=path)
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

        self.path.mkdir(parents=True, exist_ok=True)
        with open(self._pickle_path(key), "wb") as f:
            pickle.dump(value, f)

    def purge_errors(self, cls: type | tuple[type] = Exception) -> None:
        for path in self.path.glob("*.pkl"):
            cached = self._read_pickle(path)
            if isinstance(cached, cls):
                path.unlink()

    def _parquet_path(self, key: str) -> Path:
        return self.path / f"{key}.parquet"

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


class DetailsCache(BaseCache):

    """
    if the front contracts expiry is before the current date, then...
    automatically invalidate the cache for the cache.get() call / request?
    this could be run automatically when a request is made, or with purge()
    """

    def __init__(self, path: Path):
        super().__init__(path=path)

    def get(self, key: str) -> IBContractDetails | None:
        pickle_path = self._pickle_path(key)
        if not pickle_path.exists():
            return None
        return self._read_pickle(pickle_path)

    def set(self, key: str, value: IBContractDetails):
        self.path.mkdir(parents=True, exist_ok=True)

        with open(self._pickle_path(key), "wb") as f:
            pickle.dump(value, f)

    @classmethod
    def build_key(cls, **kwargs):
        c = kwargs["contract"].__dict__
        trading_class = c.get("tradingClass", None)
        symbol = c.get("symbol", None)
        exchange = c.get("exchange", None)
        secType = c.get("secType", None)
        expiry = c.get("lastTradeDateOrContractMonth", None)
        currency = c.get("currency", None).strip()
        parts = [trading_class, symbol, exchange, secType, expiry, currency]
        parts = [part for part in parts if part != ""]
        key = "-".join(parts)
        return key


class CachedFunc:
    def __init__(self, func: Callable, cache: RequestsCache | DetailsCache):
        self._func = func
        self._cache = cache

        self._log = LoggerAdapter.from_name(name=type(self).__name__)

    async def __call__(self, *args, **kwargs) -> list[Any] | Exception:
        assert args == (), "Keywords arguments only"

        key = self._cache.build_key(*args, **kwargs)

        cached = self._cache.get(key)
        if cached is not None:
            self._log.debug(f"Returning cached {key}={self._value_to_str(cached)}")
            if isinstance(cached, Exception):
                raise cached
            else:
                return cached

        self._log.debug(f"No cached {key}")

        try:
            result = await self._func(**kwargs)
            self._cache.set(key, result)
            self._log.debug(f"Saved {self._value_to_str(result)} items...")
            return result
        except Exception as e:
            self._cache.set(key, e)
            self._log.debug(f"Saved {e} items...")
            raise

    def is_cached(self, *args, **kwargs) -> bool:
        assert args == (), "Keywords arguments only"
        key = self._cache.build_key(**kwargs)
        cached = self._cache.get(key)
        return cached is not None

    @staticmethod
    def _value_to_str(value: Exception | list) -> str:
        if isinstance(value, Exception):
            return repr(value)
        elif isinstance(value, list):
            return f"{len(value)} items"
        else:
            raise NotImplementedError
