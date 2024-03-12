import asyncio
import time
from collections import deque
from pathlib import Path

import pandas as pd
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import WhatToShow
from pyfutures.client.cache import CachedFunc
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.objects import ClientException
from pyfutures.client.parsing import ClientParser
from pyfutures.logger import LoggerAdapter


class InteractiveBrokersBarClient:
    def __init__(
        self,
        client: InteractiveBrokersClient,
        delay: float = 0,
        cache_dir: Path | None = None,
        use_cache: bool = True,
    ):
        self._client = client
        self._delay = delay
        self._log = LoggerAdapter.from_name(name=type(self).__name__)

        self._use_cache = use_cache

        if use_cache:
            assert cache_dir is not None
            self.cache = CachedFunc(
                self._client.request_bars,
                cache_dir=cache_dir,
            )
        else:
            self.cache = None

        self._parser = ClientParser()

    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration | None = None,
        start_time: pd.Timestamp | None = None,
        end_time: pd.Timestamp | None = None,
        as_dataframe: bool = False,
        limit: int | None = None,
        skip_first: bool = False,
    ):
        # assert is_unqualified_contract(contract)

        # assert start_time is not None and end_time is not None  # TODO
        # TODO: floor start_time and end_time to second
        # TODO: check start_time is >= head_timestamp
        if end_time is None:
            end_time = pd.Timestamp.utcnow()

        if start_time is None:
            self._log.info(f"requesting head_timestamp for {contract.tradingClass}")
            start_time = await self._client.request_head_timestamp(
                contract=contract,
                what_to_show=what_to_show,
            )
            self._log.info(f"head_timestamp: {start_time}")

        assert start_time < end_time
        
        if duration is None:
            duration = bar_size.to_appropriate_duration()
            
        interval = duration.to_timedelta()

        if skip_first:
            end_time = end_time.floor(interval)
        else:
            end_time = end_time.ceil(interval)

        total_bars = deque()
        
        i = 0
        while end_time > start_time:
            
            self._log.info(f"{contract} | {end_time - interval} -> {end_time}")
                
            bars: list[BarData] = await self._request_bars(
                contract=contract,
                bar_size=bar_size,
                what_to_show=what_to_show,
                duration=duration,
                end_time=end_time,
                use_cache=self._use_cache if skip_first else (self._use_cache and i > 0),
            )
            bars = [b for b in bars if b.timestamp >= start_time]
            
            if len(bars) > 0:
                self._log.debug(f"---> Downloaded {len(bars)} bars. {bars[0].timestamp} {bars[-1].timestamp}")
            else:
                self._log.debug("---> Downloaded 0 bars.")
            
            total_bars.extendleft(bars)
            
            end_time = end_time - interval

            i += 1
            
            if limit and len(total_bars) >= limit:
                total_bars = list(total_bars)[-limit:]  # last x number of bars in the list
                break
        
        if as_dataframe:
            df = pd.DataFrame([self._parser.bar_data_to_dict(obj) for obj in total_bars])
            return df
        
        return total_bars
        
    async def _request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
        use_cache: bool,
    ):
        kwargs = dict(
            contract=contract,
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=duration,
            end_time=end_time,
        )

        if use_cache:
            is_cached = self.cache.is_cached(**kwargs)
            func = self.cache
        else:
            func = self._client.request_bars
            is_cached = False
        
        # fetch bars
        start = time.perf_counter()
        bars = []
        try:
            bars: list[BarData] = await func(**kwargs)
        except ClientException as e:
            self._log.error(str(e))
        except asyncio.TimeoutError as e:
            self._log.error(str(e.__class__.__name__))
        stop = time.perf_counter()
        self._log.debug(f"Elapsed time: {stop - start:.2f}")
        
        if self._delay > 0 and not is_cached:
            self._log.debug(f"Waiting for {self._delay} seconds...")
            await asyncio.sleep(self._delay)

        return bars

    async def request_quote_ticks(
        self,
        contract: IBContract,
        start_time: pd.Timestamp = None,
        end_time: pd.Timestamp = None,
        as_dataframe: bool = False,
    ):
        assert start_time is not None and end_time is not None

        freq = pd.Timedelta(seconds=60)
        timestamps = pd.date_range(start=start_time, end=end_time, freq=freq, tz="UTC")[::-1]
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
                assert quote.timestamp <= end_time
                assert quote.timestamp >= start_time

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
                assert quote.timestamp < end_time

            quotes = [q for q in quotes if q.timestamp >= start_time]

            results = quotes + results

            if len(quotes) < count:
                break

            end_time = quotes[0].timestamp
            self._log.debug(f"End time: {end_time}")

            if self._delay > 0:
                await asyncio.sleep(self._delay)

        for quote in results:
            assert quote.timestamp >= start_time

        timestamps = [quote.timestamp for quote in quotes]
        assert pd.Series(timestamps).is_monotonic_increasing

        if as_dataframe:
            return pd.DataFrame([self._parser.historical_tick_bid_ask_to_dict(obj) for obj in results])
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
