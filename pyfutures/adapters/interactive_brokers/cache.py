import asyncio
from typing import Any
import pandas as pd
from typing import Callable
from nautilus_trader.common.component import Logger

from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pathlib import Path
from pyfutures.adapters.interactive_brokers.parsing import unqualified_contract_to_instrument_id
from nautilus_trader.common.component import Logger
import pickle
import json
from ibapi.contract import Contract as IBContract

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
        self._log = Logger("HistoricCache")
    
    async def __call__(self, *args, **kwargs) -> list[Any] | asyncio.TimeoutError | ClientException:
        
        assert args == (), "Keywords arguments only"
        
        self._cachedir.mkdir(parents=True, exist_ok=True)
        
        key = self._build_key(*args, **kwargs)
        
        cached = self._get(key)
        if cached is not None:
            return cached
        
        result = await self._func(**kwargs)
        
        self._set(key, result)
        
        return result
    
    def purge_errors(self):
        # delete iterate json files in the cache dir
        pass
    
    
    
    def _get(
        self,
        key: str,
    ) -> list[Any] | asyncio.TimeoutError | ClientException | None:
        
        path = self._pickle_path(key)
        if path.exists():
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    def _set(
        self,
        key: str,
        value: list[Any] | asyncio.TimeoutError | ClientException,
    ) -> None:
        
        if isinstance(value, (list | asyncio.TimeoutError | ClientException)):
            with open(self._pickle_path(key), "wb") as f:
                pickle.dump(value, f)
        else:
            raise RuntimeError(f"Unsupported type {type(value).__name__}")
    
    def get_cached_path(self, *args, **kwargs) -> Path:
        assert args == (), "Keywords arguments only"
        return self._pickle_path(self._build_key(**kwargs))
    
    def _pickle_path(self, key: str) -> Path:
        return self._cachedir / f"{key}.pkl"
    
    def _build_key(
        self,
        **kwargs
    ):
        # https://blog.xam.de/2016/07/standard-format-for-time-stamps-in-file.html
        """
        contract: IBContractDetails,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
        """
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
        
        return self._sanitize_filename(key)
    
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