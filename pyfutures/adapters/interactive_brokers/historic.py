import asyncio
from collections import deque
from nautilus_trader.adapters.interactive_brokers.common import IBContractDetails

import pandas as pd
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from nautilus_trader.common.component import Logger

from pyfutures.adapters.interactive_brokers.client.objects import ClientException

# from pyfutures.adapters.interactive_brokers.client.objects import TimeoutError
from pyfutures.adapters.interactive_brokers.client.client import (
    InteractiveBrokersClient,
)
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import bar_data_to_dict
from pyfutures.adapters.interactive_brokers.parsing import (
    historical_tick_bid_ask_to_dict,
)
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime

from pathlib import Path
from pyfutures.adapters.interactive_brokers.parsing import (
    contract_details_to_instrument_id,
)

from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
import pickle
import json
import logging
from typing import Iterable

# from pyfutures.tests.exporter.cache import CachedFunc
#
# from aiocache.serializers import NullSerializer
# from pyfutures.tests.exporter.cache import PyfuturesCache
# from aiocache import Cache
# from aiocache import caches
# from pyfutures.tests.exporter.cache import request_bars_cached


class HistoricCache:
    """
    Creates a cache
    name: str -> the subdirectory of the cache, eg request_bars, request_quote_ticks, request_trade_ticks
    """

    def __init__(self, name: str):
        self.cachedir = Path.home() / "Desktop" / "download_cache" / name
        # self._log = logging.getLogger("HistoricCache").setLevel(logging.DEBUG)
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
                # f"Cached TimeoutError with timeout_seconds={timeout_seconds}"
                #
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


class InteractiveBrokersHistoric:
    def __init__(
        self,
        client: InteractiveBrokersClient,
        delay: float = 0,
    ):
        self._client = client
        self._log = Logger(type(self).__name__)
        self._delay = delay

    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        start_time: pd.Timestamp = None,
        end_time: pd.Timestamp = None,
        as_dataframe: bool = False,
        limit: int = None,
        cache: bool = True,
    ):
        is_cached = False
        if cache:
            request_bars = RequestBarsCache(
                client=self._client, name="request_bars", timeout_seconds=100
            )
        else:
            request_bars = self._client.request_bars
        # assert start_time is not None and end_time is not None  # TODO
        # TODO: floor start_time and end_time to second
        # TODO: check start_time is >= head_timestamp
        assert limit is None  # TODO

        if end_time is None:
            end_time = pd.Timestamp.utcnow()

        # https://groups.io/g/insync/topic/adding_contfut_to_ib_insync/5850800?p=
        # detail = await self._client.request_front_contract_details(contract)
        detail = IBContractDetails(contract=contract)

        head_timestamp = await self._client.request_head_timestamp(
            contract=contract,
            what_to_show=what_to_show,
        )
        self._log.info(f"--> req_head_timestamp: {head_timestamp}")
        if start_time is None or start_time < head_timestamp:
            start_time = head_timestamp

        total_bars = deque()
        duration = self._client._get_appropriate_duration(bar_size)
        interval = duration.to_timedelta()
        while end_time >= start_time:
            self._log.info(
                f"========== ({contract.symbol}.{contract.exchange}) =========="
                f"========== {end_time} -> {end_time - interval} ==========",
            )

            bars = []
            request_bars_params = dict(
                bar_size=bar_size,
                what_to_show=what_to_show,
                duration=duration,
                end_time=end_time,
            )

            if cache:
                request_bars_params["detail"] = detail
                is_cached = request_bars.in_cache(**request_bars_params)
            else:
                request_bars_params["contract"] = detail.contract

            try:
                bars: list[BarData] = await request_bars(**request_bars_params)
                # print([bar.timestamp for bar in bars])
            except ClientException as e:
                self._log.error(str(e))
            except asyncio.TimeoutError as e:
                self._log.error(str(e.__class__.__name__))
            else:
                total_bars.extendleft(bars)
                if len(bars) > 0:
                    first_date = bars[0].date
                    last_date = bars[-1].date
                    first_timestamp = bars[0].timestamp
                    last_timestamp = bars[-1].timestamp
                    self._log.info(f"---> Retrieved {len(bars)} bars...")
                    self._log.info(f"---> bars[0] {first_date} {last_timestamp}")
                    self._log.info(f"---> bars[-1] {last_date} {first_timestamp}")

            end_time = end_time - interval

            if not is_cached and self._delay > 0:
                await asyncio.sleep(self._delay)

        if as_dataframe:
            df = pd.DataFrame([bar_data_to_dict(obj) for obj in total_bars])
            df["volume"] = df.volume.astype(float)
            return df

        return total_bars

    async def request_quote_ticks(
        self,
        contract: IBContract,
        start_time: pd.Timestamp = None,
        end_time: pd.Timestamp = None,
        as_dataframe: bool = False,
    ):
        assert start_time is not None and end_time is not None

        freq = pd.Timedelta(seconds=60)
        timestamps = pd.date_range(start=start_time, end=end_time, freq=freq, tz="UTC")[
            ::-1
        ]
        results = []

        for i in range(len(timestamps) - 2):
            start_time = timestamps[i]
            end_time = timestamps[i + 1]
            self._log.debug(f"Requesting: {start_time} > {end_time}")

            quotes: list[HistoricalTickBidAsk] = await self._client.request_quote_ticks(
                contract=contract,
                start_time=start_time,
                end_time=end_time,
                count=1000,
            )

            assert len(quotes) <= 1000
            for quote in quotes:
                # print(start_time, end_time)
                # print(parse_datetime(quote.time))
                print(quote)
                assert parse_datetime(quote.time) <= end_time
                assert parse_datetime(quote.time) >= start_time

            exit()
            if self._delay > 0:
                await asyncio.sleep(self._delay)

        if as_dataframe:
            return pd.DataFrame([obj for obj in results])

        return results

    async def request_quote_ticks2(
        self,
        contract: IBContract,
        start_time: pd.Timestamp = None,
        end_time: pd.Timestamp = None,
        as_dataframe: bool = False,
    ):
        """
        if end_time is passed only:
            start_time=None, end_time=end_time
            first_tick = x
            break on first tick timestamp

        if start_time is passed only:
            start_time=start_time, end_time=None
            last_tick = x
            break on last tick timestamp

        if start_time is passed and end_time is passed:
            start_time=None, end_time=end_time
            first_tick = x
            break on first tick timestamp
        """
        assert start_time is not None and end_time is not None

        results = []

        i = 0
        count = 1000
        while True:
            # split time range into interval, floor start and ceil end
            # for each interval, reduce end time until empty list
            self._log.debug(f"Requesting: {end_time}")
            quotes: list[HistoricalTickBidAsk] = await self._client.request_quote_ticks(
                contract=contract,
                # start_time=start_time,
                end_time=end_time,
                count=count,
            )

            for quote in quotes:
                assert parse_datetime(quote.time) < end_time

            quotes = [q for q in quotes if parse_datetime(q.time) >= start_time]

            results = quotes + results

            if len(quotes) < count:
                break

            end_time = parse_datetime(quotes[0].time)
            self._log.debug(f"End time: {end_time}")

            if self._delay > 0:
                await asyncio.sleep(self._delay)

        for quote in results:
            assert parse_datetime(quote.time) >= start_time

        timestamps = [parse_datetime(quote.time) for quote in quotes]
        assert pd.Series(timestamps).is_monotonic_increasing

        if as_dataframe:
            return pd.DataFrame(
                [historical_tick_bid_ask_to_dict(obj) for obj in results]
            )
        return results


# caches.set_config(
#     {
#         "default": {
#             "cache": "aiocache.SimpleMemoryCache",
#             "serializer": {"class": "aiocache.serializers.StringSerializer"},
#         },
#         "request_bars": {
#             "cache": "pyfutures.tests.exporter.cache.PyfuturesCache",
#             "serializer": {
#                 "class": "aiocache.serializers.NullSerializer",
#             },
#             "key_builder": "pyfutures.tests.exporter.cache.key_builder",
#         },
#     }
# )
# cache_location = Path.home() / "Desktop" / "historic_cache"
# memory = Memory(location=cache_location, verbose=100)


# https://github.com/joblib/joblib/issues/889#issuecomment-1840865997
# def get_cached_function(memory, func, *margs, **mkargs):
#     cached_func = memory.cache(func, *margs, **mkargs)
#
#     def outer_wrapper(func):
#         async def inner_wrapper(*args, **kwargs):
#             func_id, args_id = cached_func._get_output_identifiers(*args, **kwargs)
#             path = [func_id, args_id]
#             print("CHECK CALL IN CACHE")
#             print(func_id, args_id)
#             print(cached_func.check_call_in_cache(*args, **kwargs))
#
#             if not cached_func._is_in_cache_and_valid(path=path):
#                 print("RUNNING FUNC AGAIN")
#                 output = await func(*args, **kwargs)
#                 cached_func.store_backend.dump_item(
#                     path, output, verbose=cached_func._verbose
#                 )
#             else:
#                 print("item found in cache...")
#             print("LOAD ITEM")
#             result = cached_func.store_backend.load_item(path=path)
#             print("END LOAD ITEM")
#
#             return result
#         return inner_wrapper
#
#     return outer_wrapper
#


# @cached(ttl=10, cache=RedisCache, serializer=PickleSerializer())
# async def request_bars(
#     self,
#     contract: IBContract,
#     conId: str,
#     bar_size: BarSize,
#     what_to_show: WhatToShow,
#     duration: Duration,
#     end_time: pd.Timestamp,
# ):
#     try:
#         bars = await self._client.request_bars(
#             contract=contract,
#             bar_size=str(bar_size),
#             what_to_show=what_to_show,
#             duration=duration,
#             end_time=end_time,
#             timeout_seconds=25,
#         )
#         return bars
#     except ClientException as e:
#         return str(e)
#     except asyncio.TimeoutError as e:
#         return str(e)
#

# def request_bars_sync(
#     contract=contract,
#     conId=detail.contract.conId,
#     bar_size=str(bar_size),
#     what_to_show=what_to_show,
#     duration=duration,
#     end_time=end_time,
# ):
#     # cached_func = memory.cache(func)
#     async def request_bars_async(*args, **kwargs):
#         func_id, args_id = cached_func._get_output_identifiers(*args, **kwargs)
#         path = [func_id, args_id]
#         print("CHECK CALL IN CACHE")
#         print(func_id, args_id)
#         print(cached_func.check_call_in_cache(*args, **kwargs))
#
#         if not cached_func._is_in_cache_and_valid(path=path):
#             print("RUNNING FUNC AGAIN")
#             output = await func(*args, **kwargs)
#             cached_func.store_backend.dump_item(
#                 path, output, verbose=cached_func._verbose
#             )
#         else:
#             print("item found in cache...")
#         print("LOAD ITEM")
#         result = cached_func.store_backend.load_item(path=path)
#         print("END LOAD ITEM")
#
#         return result
#
#     return request_bars_async


# class AsyncMemorizedFunc(MemorizedFunc):
#     def __init__(self, func, *args, **kwargs):
#         super().__init__(func=func, *args, **kwargs)
#         self.func = outer_wrapper(func)


# async def request_bars(
#     self,
#     contract: IBContract,
#     bar_size: BarSize,
#     what_to_show: WhatToShow,
#     start_time: pd.Timestamp = None,
#     end_time: pd.Timestamp = None,
#     as_dataframe: bool = False,
# ):
#     """
#     Pacing Violations for Small Bars (30 secs or less)
#
#     Although Interactive Brokers offers our clients high quality market data, IB is not a specialised market data provider and as such it is forced to put in place restrictions to limit traffic which is not directly associated to trading. A Pacing Violation1 occurs whenever one or more of the following restrictions is not observed:
#
#     Making identical historical data requests within 15 seconds.
#     Making six or more historical data requests for the same Contract, Exchange and Tick Type within two seconds.
#     Making more than 60 requests within any ten minute period.
#     """
#     # TODO: check start_date is >= head_timestamp, otherwise use head_timestamp
#     # TODO: get first bars date
#
#     # assert bar_size in (BarSize._1_DAY, BarSize._1_HOUR, BarSize._1_MINUTE)
#
#     duration = self._get_appropriate_duration(bar_size)
#     freq = duration.to_timedelta()
#
#     total_bars = []
#
#     end_time_ceil = end_time.ceil(freq)
#     start_time_floor = end_time_ceil - freq
#
#     while end_time_ceil > start_time_floor:
#         print(contract.symbol, contract.exchange)
#
#         self._log.debug(
#             f"--> ({contract.symbol}{contract.exchange}) "
#             f"Downloading bars at interval: {start_time_floor} > {end_time_ceil}",
#         )
#
#         try:
#             bars: list[BarData] = await self._client.request_bars(
#                 contract=contract,
#                 bar_size=bar_size,
#                 what_to_show=what_to_show,
#                 duration=duration,
#                 end_time=end_time_ceil,
#                 timeout_seconds=100,
#             )
#
#             print(f"Downloaded {len(bars)} bars...")
#
#             # for b in bars:
#             #     print(parse_datetime(b.date))
#
#             # bars can be returned that are outside the end_time - durationStr when there is no data within the range
#             bars = [
#                 b
#                 for b in bars
#                 if parse_datetime(b.date) >= start_time_floor
#                 and parse_datetime(b.date) < end_time_ceil
#             ]
#             print(f"Filtered {len(bars)} bars...")
#
#         except ClientException as e:
#             if e.code != 162:
#                 raise e
#
#             # Historical Market Data Service error message:HMDS query returned no data
#             if self._delay > 0:
#                 await asyncio.sleep(self._delay)
#
#             end_time_ceil -= freq
#             start_time_floor -= freq
#             print(f"No data for end_time {end_time_ceil}")
#
#             continue
#
#         total_bars = bars + total_bars
#
#         assert pd.Series(
#             [parse_datetime(b.date) for b in total_bars]
#         ).is_monotonic_increasing
#
#         end_time_ceil -= freq
#         start_time_floor -= freq
#
#         if self._delay > 0:
#             await asyncio.sleep(self._delay)
#
#     total_bars = [
#         b
#         for b in total_bars
#         if parse_datetime(b.date) >= start_time
#         and parse_datetime(b.date) < end_time
#     ]
#
#     if as_dataframe:
#         return pd.DataFrame([bar_data_to_dict(obj) for obj in total_bars])
#
#     return total_bars

# @staticmethod
# def _parse_dataframe(bars: list[BarData]) -> pd.DataFrame:

#     return pd.DataFrame(
#         {
#             "date": [parse_datetime(bar.date) for bar in bars],
#             "open": [bar.open for bar in bars],
#             "high": [bar.high for bar in bars],
#             "low": [bar.low for bar in bars],
#             "close": [bar.close for bar in bars],
#             "volume": [float(bar.volume) for bar in bars],
#             "wap": [float(bar.wap) for bar in bars],
#             "barCount": [bar.barCount for bar in bars],
#         },
#     )

# quotes: list[IBQuoteTick] = await self._client.request_quote_ticks(
#     name=str(UUID4()),
#     contract=contract,
#     start_time=head_timestamp + pd.Timedelta(days=200),
#     # end_time=head_timestamp + pd.Timedelta(days=14),
#     count=1,
# )
# start_time = quotes[0].time
# assert start_time < end_time
