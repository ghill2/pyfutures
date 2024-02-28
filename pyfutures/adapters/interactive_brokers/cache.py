import asyncio
from nautilus_trader.adapters.interactive_brokers.common import IBContractDetails
import pandas as pd
from ibapi.common import BarData
from nautilus_trader.common.component import Logger

from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pathlib import Path
from pyfutures.adapters.interactive_brokers.parsing import contract_details_to_instrument_id

from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
import pickle
import json
import logging
from typing import Iterable

class HistoricCache:
    """
    Creates a cache
    name: str -> the subdirectory of the cache, eg request_bars, request_quote_ticks, request_trade_ticks
    """

    def __init__(self, name: str):
        self.cachedir = Path.home() / "Desktop" / "download_cache" / name
        self._log = Logger("HistoricCache")

    def in_cache(self, *args, **kwargs):
        key = self.key_builder(*args, **kwargs)
        pkl_path = self.cachedir / f"{key}.pkl"
        json_path = self.cachedir / f"{key}.json"
        return pkl_path.exists() or json_path.exists()

    def get_data(self, key):
        pkl_path = self.cachedir / f"{key}.pkl"
        if pkl_path.exists():
            self._log.debug(
                f"get data: cached data exists: returning: {key}", LogColor.BLUE
            )

            with open(pkl_path, "rb") as f:
                return pickle.load(f)
        return False

    def get_errors(self, key):
        json_path = self.cachedir / f"{key}.json"
        if json_path.exists():
            with open(json_path, "r") as f:
                cached_error = json.load(f)
                self._log.debug(
                    f"get_errors: cached error response exists: {cached_error=} {key}",
                    LogColor.BLUE,
                )
                return cached_error

        return False

    def set_data(self, key, value: Iterable):
        self._log.debug(f"set_data: storing data in cache: {len(value)}", LogColor.BLUE)
        pkl_path = self.cachedir / f"{key}.pkl"
        with open(pkl_path, "wb") as f:
            pickle.dump(value, f)

    def set_errors(self, key, value):
        self._log.debug(
            f"set_errors: storing error response in cache: {value=}", LogColor.BLUE
        )
        json_path = self.cachedir / f"{key}.json"
        serialized_value = json.dumps(value, indent=4)  # Serialize to a string
        with open(json_path, "w") as f:
            f.write(serialized_value)

    
class RequestBarsCache(HistoricCache):
    def __init__(
        self, client: InteractiveBrokersClient, name: str, timeout_seconds: int
    ):
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._log = Logger("RequestsCache")
        super().__init__(name)
        # names = logging.Logger.manager.loggerDict
        # for name in names:
        #     if "ibapi" in name:
        #         logging.getLogger(name).setLevel(api_log_level)
        #

    @staticmethod
    def key_builder(
        detail: IBContractDetails,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
    ):
        # https://blog.xam.de/2016/07/standard-format-for-time-stamps-in-file.html
        instrument_id = contract_details_to_instrument_id(detail)
        end_time_str = end_time.isoformat().replace(":", "_")
        start_time = end_time - duration.to_timedelta()
        start_time_str = start_time.isoformat().replace(":", "_")
        return f"{instrument_id}-{what_to_show.value}-{str(bar_size)}-{duration.value}-{start_time_str}-{end_time_str}"

    async def __call__(
        self,
        detail: IBContractDetails,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
    ):
        
        self.cachedir.mkdir(parents=True, exist_ok=True)

        key = self.key_builder(
            detail=detail,
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=duration,
            end_time=end_time,
        )

        if cached_response := self.get_data(key):
            return cached_response

        if cached_response := self.get_errors(key):
            if cached_response["type"] == "ClientException":
                raise ClientException(
                    code=cached_response["code"], message=cached_response["message"]
                )
            if cached_response["type"] == "TimeoutError":
                timeout_seconds = cached_response["timeout_seconds"]
                raise asyncio.TimeoutError()
        try:
            bars: list[BarData] = await self._client.request_bars(
                contract=detail.contract,
                bar_size=bar_size,
                what_to_show=what_to_show,
                duration=duration,
                end_time=end_time,
                timeout_seconds=self._timeout_seconds,
            )
        except ClientException as e:
            self.set_errors(
                key, dict(type="ClientException", code=e.code, message=e.message)
            )
            raise e
        except asyncio.TimeoutError as e:
            self.set_errors(
                key,
                dict(type="TimeoutError", timeout_seconds=self._timeout_seconds),
            )
            raise e

        if bars is not None:
            self.set_data(key, bars)
            return bars
        
    # def purge_errors(self):
        
        