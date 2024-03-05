import asyncio
from typing import Any
import pandas as pd
from typing import Callable
from nautilus_trader.common.component import Logger

from pyfutures.adapter.client.objects import ClientException
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.enums import WhatToShow
from pathlib import Path
from pyfutures.adapter.parsing import unqualified_contract_to_instrument_id
from nautilus_trader.common.component import Logger
import pickle
import json
from ibapi.contract import Contract as IBContract
from nautilus_trader.core.rust.common import LogColor

class CachedFunc:
    """
    Creates a cache
    name: str -> the subdirectory of the cache, eg request_bars, request_quote_ticks, request_trade_ticks
    """

    def __init__(
        self,
        func: Callable,
    ):
        self._func = func
        self._cachedir = Path.home() / "Desktop" / "download_cache" / func.__name__
        self._log = Logger(f"{func.__name__}Cache")
    
    async def __call__(self, *args, **kwargs) -> list[Any] | Exception:
        
        assert args == (), "Keywords arguments only"
        
        self._cachedir.mkdir(parents=True, exist_ok=True)
        
        key = self._build_key(*args, **kwargs)
        
        cached = self._get(key)
        if cached is not None:
            
            self._log.debug(f"Returning cached {key}={self._value_to_str(cached)}", LogColor.BLUE)
            if isinstance(cached, Exception):
                raise cached
            else:
                return cached
        
        self._log.debug(f"No cached {key}", LogColor.BLUE)
        
        try:
            result = await self._func(**kwargs)
            self._set(key, result)
            self._log.debug(f"Saved {self._value_to_str(result)} items...", LogColor.BLUE)
            return result
        except ClientException as e:
            self._log.error(str(e))
            self._set(key, e)
            self._log.debug(f"Saved {e} items...", LogColor.BLUE)
            raise
        except asyncio.TimeoutError as e:
            self._log.error(str(e.__class__.__name__))
            self._set(key, e)
            raise
        
        raise NotImplementedError()
    
    @staticmethod
    def _value_to_str(value: Exception | list) -> str:
        return repr(value) if isinstance(value, Exception) else f"{len(value)} items"
    
    def purge_errors(self, cls: type | tuple[type] = Exception) -> None:
        for path in self._cachedir.rglob("*.pkl"):
            with open(path, "rb") as f:
                cached = pickle.load(f)
            if isinstance(cached, cls):
                path.unlink()
    
    def _get(
        self,
        key: str,
    ) -> list[Any] | Exception | None:
        
        path = self._pickle_path(key)
        if path.exists():
            with open(path, "rb") as f:
                cached = pickle.load(f)
                return cached
                
        return None
    
    def _set(
        self,
        key: str,
        value: list[Any] | Exception,
    ) -> None:
        
        if not isinstance(value, (list, Exception)):
            raise RuntimeError(f"Unsupported type {type(value).__name__}")
        
        with open(self._pickle_path(key), "wb") as f:
            pickle.dump(value, f)
    
    def is_cached(self, *args, **kwargs) -> bool:
        assert args == (), "Keywords arguments only"
        return self.get_cached_path(*args, **kwargs).exists()
    
    def get_cached_path(self, *args, **kwargs) -> Path:
        assert args == (), "Keywords arguments only"
        return self._pickle_path(self._build_key(**kwargs))
    
    def _pickle_path(self, key: str) -> Path:
        return self._cachedir / f"{key}.pkl"
    
    @classmethod
    def _build_key(
        cls,
        **kwargs
    ):
        parsing = {
            IBContract: lambda x: str(unqualified_contract_to_instrument_id(x)),
            pd.Timestamp: lambda x: x.strftime("%Y-%m-%d %H%M%S"),
            Duration: lambda x: x.value,
            BarSize: lambda x: str(x),
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
        
        key = "-".join(parts)
        
        return cls._sanitize_filename(key)
    
    @staticmethod
    def _sanitize_filename(filename):
        """
        Sanitize a string value for safe storage in a file name
        across Windows, Linux, and macOS operating systems.
        """
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        sanitized_filename = filename
        
        # Replace illegal characters with underscore (_)
        for char in illegal_chars:
            sanitized_filename = sanitized_filename.replace(char, '_')
        
        # Remove leading and trailing whitespaces and dots
        sanitized_filename = sanitized_filename.strip().strip('.')
        
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