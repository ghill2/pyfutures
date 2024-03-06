import asyncio
from typing import Any
import pandas as pd
from typing import Callable
from nautilus_trader.common.component import Logger

from pyfutures.client.objects import ClientException
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.enums import WhatToShow
from pathlib import Path
from pyfutures.adapter.parsing import unqualified_contract_to_instrument_id
from nautilus_trader.common.component import Logger
import pickle
import json
from pyfutures.client.parsing import bar_data_to_dict
from pyfutures.client.parsing import bar_data_from_dict
from ibapi.contract import Contract as IBContract
from nautilus_trader.common.component import Logger
from nautilus_trader.core.rust.common import LogColor

from pyfutures.adapter.enums import BarSize, Duration, WhatToShow
from pyfutures.adapter.parsing import unqualified_contract_to_instrument_id
from pyfutures.client.objects import ClientException

# Create a color formatter with blue for INFO messages
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)s:%(name)s:%(message)s %(reset)s",
    log_colors={
        'DEBUG': 'blue',
        'INFO': 'blue',  # Set blue for INFO messages
        'WARNING': 'blue',
        'ERROR': 'blue',
        'CRITICAL': 'blue,blue'
    }
)

# Create a stream handler and set the formatter
handler = logging.StreamHandler()
handler.setFormatter(formatter)

class CachedFunc:
    """
    Creates a cache
    name: str -> the subdirectory of the cache, eg request_bars, request_quote_ticks, request_trade_ticks
    """

    def __init__(
        self,
        func: Callable,
        cachedir: Path | None = None,
        log_level: int = logging.INFO
    ):
        path = cachedir or (Path.home() / "Desktop" / "download_cache" / func.__name__)
        self.cache = HistoricCache(path)
        
        self._func = func
        self._cachedir = cachedir or (Path.home() / "Desktop" / "download_cache" / func.__name__)
        self._log = colorlog.getLogger(self.__class__.__name__)
        self._log.addHandler(handler)
        self._log.setLevel(log_level)
    
    async def __call__(self, *args, **kwargs) -> list[Any] | Exception:
        
        assert args == (), "Keywords arguments only"
        
        self.cache.path.mkdir(parents=True, exist_ok=True)
        
        key = self.build_key(*args, **kwargs)
        
        cached = self.cache.get(key)
        if cached is not None:
            
            self._log.debug(f"Returning cached {key}={self._value_to_str(cached)}")
            if isinstance(cached, Exception):
                raise cached
            else:
                return cached
        
        self._log.debug(f"No cached {key}")
        
        try:
            result = await self._func(**kwargs)
            self._set(key, result)
            self._log.debug(f"Saved {self._value_to_str(result)} items...")
            return result
        except Exception as e:
            self._log.error(str(e))
            self._set(key, e)
            self._log.debug(f"Saved {e} items...")
            raise
    
    @classmethod
    def build_key(
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
    
    def is_cached(self, *args, **kwargs) -> bool:
        assert args == (), "Keywords arguments only"
        key = self.build_key(**kwargs)
        cached = self.cache.get(key)
        return cached is not None
    
    @staticmethod
    def _value_to_str(value: Exception | list) -> str:
        
        if isinstance(value, Exception):
            return repr(value)
        elif isinstance(value, list):
            return f"{len(value)} items"
        
        raise NotImplementedError()
    
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
