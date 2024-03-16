from __future__ import annotations

import datetime
from collections.abc import Iterable
from typing import Annotated

import numpy as np
import pandas as pd
import pytz
from msgspec import Meta


# An integer constrained to values < 0
NegativeInt = Annotated[int, Meta(lt=0)]


class MarketSchedule:
    def __init__(
        self,
        name: str,
        data: pd.DataFrame,
        timezone: pytz.timezone,
    ):
        """
        Excepts data to be in the format:
        dayofweek: (integer 0 > 6)
        open: datetime.time()
        close: datetime.time()
        """
        self.data = data

        self._name = name
        self._zoneinfo = timezone.zone
        self._timezone = timezone

        # TODO: removes duplicates
        # TODO: check for overlapping times
        # TODO: sort by date and then open time
        # TODO: check open and close are same day
        # TODO: ensure integer index
        # TODO: ensure no missing days, close days have time 00:00 to 00:00
        # TODO: no timezone information before localizing
        # TODO assert open > close
        # TODO assert columns == dayofweek, open, close
        # TODO assert columns types == int, datetime.time, datetime.time
        # TODO assert dayofweek in range(7)
        # TODO assert input types

    def open_delta(self, dayofweek: int) -> pd.Timedelta:
        open_times = self.data[self.data.dayofweek == dayofweek].open
        open_time = min(open_times)
        return pd.Timedelta(hours=open_time.hour, minutes=open_time.minute)

    def is_open(self, now: pd.Timestamp) -> bool:
        now = now.tz_convert(self._timezone)

        now_time = now.time()

        mask = (now_time >= self.data.open) & (now_time < self.data.close) & (now.dayofweek == self.data.dayofweek)

        return mask.any()

    def is_open(self, timestamp: pd.Timestamp) -> bool:
        # TODO: assert utz timezone

        local = timestamp.tz_convert(self._timezone)
        local_time = timestamp.time()
        mask = (local_time >= self.data.open) & (local_time < self.data.close) & (local.dayofweek == self.data.dayofweek)
        return mask.any()

    def is_open_list(self, timestamps: Iterable[pd.Timestamp]) -> pd.DatetimeIndex:
        # TODO: assert utz timezone

        locals = timestamps.tz_convert(self._timezone)
        local_times = timestamps.time

        mask = np.zeros(len(timestamps), dtype=bool)
        sessions = self.data.itertuples()
        for session in sessions:
            _mask = (local_times >= session.open) & (local_times < session.close) & (locals.dayofweek == session.dayofweek)
            mask = mask | _mask
        return timestamps[mask]  # utc

        raise NotImplementedError

    def is_closed(self, now: pd.Timestamp) -> bool:
        return not self.is_open(now)

    def next_open(self, now: pd.Timestamp) -> pd.Timestamp | None:
        now = now.tz_convert(self._timezone)

        now_time = now.time()

        dayofweek = now.dayofweek

        sessions = self.data[self.data.dayofweek == dayofweek]
        day_diff = 0

        if not sessions.empty and now_time < sessions.iloc[-1].open:
            open_time = self.data[self.data.open > now_time].iloc[0].open

        else:
            while True:
                dayofweek = (dayofweek + 1) % 7
                day_diff += 1
                if dayofweek not in self.data.dayofweek.values:
                    continue

                sessions = self.data[self.data.dayofweek == dayofweek]
                open_time = sessions.iloc[0].open
                break

        open_day = now.floor("D") + pd.Timedelta(days=day_diff)
        open_timestamp = open_day.replace(hour=open_time.hour, minute=open_time.minute)
        return open_timestamp.tz_convert(pytz.UTC)

    def previous_trading_day(self, date: datetime.date, offset: NegativeInt) -> datetime.date:
        count = abs(offset)

        matched = 0
        while True:
            date -= datetime.timedelta(days=1)
            if date.weekday() in self.data.dayofweek.values:
                matched += 1
            if count == matched:
                break
        return date

    def time_until_close(self, now: pd.Timestamp) -> pd.Timedelta | None:
        now = now.tz_convert(self._timezone)

        now_time = now.time()

        mask = (now_time >= self.data.open) & (now_time < self.data.close) & (now.dayofweek == self.data.dayofweek)

        masked = self.data[mask]

        if masked.empty:
            return None  # market closed

        assert len(masked) == 1

        close_time = masked.close.iloc[0]
        close_timestamp = now.floor("D").replace(hour=close_time.hour, minute=close_time.minute)
        return close_timestamp - now

    def __repr__(self) -> str:
        return str(self)

    def __str__(self):
        return f"{type(self).__name__}({self._name})"

    def __getstate__(self):
        return (self._name, self.data, self._timezone)

    def __setstate__(self, state):
        self._name = state[0]
        self.data = state[1]
        self._timezone = state[2]

    def __eq__(self, other: MarketSchedule) -> bool:
        return (
            self._name == other._name,
            self.data.equals(other.data),
            self._timezone == other._timezone,
        )

    def to_date_range(
        self,
        start_date: pd.Timestamp,
        interval: pd.Timedelta,
        end_date: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        if end_date is None:
            end_date = pd.Timestamp.utcnow()
        assert start_date.tzname() == "UTC"
        assert end_date.tzname() == "UTC"

        times = pd.date_range(
            start=start_date,
            end=end_date,
            freq=interval,
        )
        return self.is_open_list(times)

    def to_open_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        # returns timestamps for every open_time in the schedule

        # TODO: assert utc input
        if end_date is None:
            end_date = pd.Timestamp.utcnow()
        days = pd.date_range(
            start=start_date,
            end=end_date,
            freq=pd.Timedelta(days=1),
        )
        days = days.tz_convert(self._timezone)

        sessions = self.data.itertuples()

        timestamps = []
        for session in sessions:
            timestamps.extend(
                day + pd.Timedelta(hours=session.open.hour, minutes=session.open.minute) for day in days[days.dayofweek == session.dayofweek]
            )
        timestamps = pd.DatetimeIndex(timestamps).sort_values().tz_convert("UTC")
        timestamps = timestamps[(timestamps >= start_date) & (timestamps < end_date)]

        return list(timestamps)

        # group sessions by dayofweek

        # for each session

    def sessions(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """
        returns sessions within a time range
        """
        if end_date is None:
            end_date = pd.Timestamp.now()
        days = pd.date_range(start=start_date, end=end_date, freq="1D")

        days = days[days.dayofweek.isin(self.data.dayofweek.values)]
        days = days.tz_localize(self._timezone)

        df = pd.DataFrame(columns=["start", "end"])
        for day in days:
            sessions = self.data[self.data.dayofweek == day.dayofweek]

            for session in sessions.itertuples():
                df.loc[len(df)] = (
                    day + pd.Timedelta(hours=session.open.hour, minutes=session.open.minute),
                    day + pd.Timedelta(hours=session.close.hour, minutes=session.close.minute),
                )
        return df

    def to_weekly_calendar_utc(self) -> pd.DataFrame:
        startofweek = pd.Timestamp("2023-11-06")
        df = self.data.copy()
        df["day"] = df["dayofweek"].apply(lambda i: startofweek + pd.Timedelta(days=i))
        df["open"] = df["open"].apply(lambda x: pd.Timedelta(hours=x.hour, minutes=x.minute))
        df["close"] = df["close"].apply(lambda x: pd.Timedelta(hours=x.hour, minutes=x.minute))
        df["open"] = (df["day"] + df["open"]).dt.tz_localize(self._timezone)
        df["close"] = (df["day"] + df["close"]).dt.tz_localize(self._timezone)
        df["name"] = self._name
        df.open = df.open.dt.tz_convert("UTC")
        df.close = df.close.dt.tz_convert("UTC")
        df.dayofweek = df.dayofweek.astype(int)
        return df

    @staticmethod
    def from_daily_str(
        name: str,
        timezone: pytz.timezone,
        value: str,
        # weekend_only: bool = False,
    ) -> MarketSchedule:
        """
        08:45-11:02, 12:30-15:02
        08:45-11:02, 12:30-15:02
        """
        data = pd.DataFrame(columns=["dayofweek", "open", "close"])
        for s in value.replace(" ", "").split(","):
            start, end = tuple(s.split("-"))
            start_hour, start_minutes = tuple(start.split(":"))
            end_hour, end_minutes = tuple(end.split(":"))

            for dayofweek in range(5):
                data.loc[len(data)] = {
                    "dayofweek": dayofweek,
                    "open": datetime.time(int(start_hour), int(start_minutes)),
                    "close": datetime.time(int(end_hour), int(end_minutes)),
                }
        data.sort_values(by=["dayofweek", "open"], inplace=True)

        return MarketSchedule(
            name=name,
            data=data,
            timezone=timezone,
        )


# class MarketCalendar:

#     def __init__(
#         self,
#         symbol: Symbol,
#         data: pd.DataFrame,
#         timezone: pytz.timezone,
#     ):

#         self.symbol = symbol
#         self._data = data
#         self._timezone = timezone

#     def __repr__(self) -> str:
#         return str(self)

#     def __str__(self):
#         return f"{type(self).__name__}({self.symbol})"

#     def is_market_open(self, now: pd.Timestamp) -> bool:
#         now = now.tz_convert(self._timezone)
#         now_time = now.time()
#         time_pairs: list[tuple[time, time]] = self._data[now.dayofweek]
#         for times in time_pairs:
#             start_time, close_time = times
#             if start_time == time(0, 0) and close_time == time(0, 0):
#                 continue
#             if now_time >= start_time and now_time < close_time:
#                 return True
#         return False

#     def market_open(self, day: pd.Timestamp) -> pd.Timestamp:
#         now = now.tz_convert(self._timezone)

#         open_time = self._data[now.dayofweek][0]

#         return now.replace(
#                 hour=open_time.hour,
#                 minute=open_time.minute,
#                 second=0,
#             )

#     def next_open(self, now: pd.Timestamp) -> pd.Timestamp:
#         # TODO
#         # get times for the day

#         # if with a range, get next range
#         # if no match, get next valid time
#         now = now.tz_convert(self._timezone)
#         now_time = now.time()

#         time_pairs: list[tuple[time, time]] = self._data[now.dayofweek]
#         for i, times in enumerate(time_pairs):
#             start_time, close_time = times

#             closed_all_day = start_time == time(0, 0) and close_time == time(0, 0)
#             last_item = i == len(time_pairs) - 1
#             in_range = now_time >= start_time and now_time < close_time

#             if closed_all_day or (in_range and last_item):

#                 day_of_week = (now.dayofweek + 1) % 6
#                 open_time = self._data[day_of_week][0][0]
#                 now += pd.Timedelta(days=1)
#                 return now.replace(
#                     hour=open_time.hour,
#                     minute=open_time.minute,
#                     second=0,
#                 )

#             elif in_range:

#                 open_time = time_pairs[i + 1]
#                 return now.replace(
#                     hour=open_time.hour,
#                     minute=open_time.minute,
#                     second=0,
#                 )


#         # if self.is_market_open(now):
#         times_ = []
#         for day_of_week, time_pairs in self._data.items():
#             for times in time_pairs:
#                 start_time, close_time = times
#                 times_.append(
#                     (day_of_week, start_time, close_time)
#                 )

#         now = now.tz_convert(self._timezone)
#         now_time = now.time()

#         for i, value in enumerate(times_):
#             day_of_week, start_time, close_time = value

#             if start_time == time(0, 0) and close_time == time(0, 0):
#                 continue

#             # is open
#             if now_time >= start_time and now_time < close_time:
#                 idx = i
#                 break

#         if idx == len(times_):
#             idx = 0

#         return pd.Timestamp(times_[idx][1])  # next open time

# @classmethod
# def _find_key_in_dict(cls, data, target_key):
#     if isinstance(data, dict):
#         if target_key in data:
#             return data[target_key]
#         for key, value in data.items():
#             result = cls._find_key_in_dict(value, target_key)
#             if result is not None:
#                 return result
#     elif isinstance(data, list):
#         for item in data:
#             result = cls._find_key_in_dict(item, target_key)
#             if result is not None:
#                 return result

#     return None  # Key not found

# @classmethod
# def from_symbol(cls, symbol: Symbol) -> DataCalendar:
#     path = PACKAGE_ROOT / f"data/calendars/{symbol}.parquet"
#     df = pd.read_parquet(path)
#     return cls(
#         data=df,
#         timezone=UniverseProvider.timezone(symbol),
#     )

# def open_time(self, day: pd.Timestamp) -> pd.Timestamp:
#     pass
#     value = value

# .ts_convert(self._timezone)
# def is_open(self, value: pd.Timestamp = None):

#     value = value.ts_convert(self._timezone)

#     if value not in self._data.index:
#         return False


#     self._data[value]
#     return ts >= df["open"][i] and ts <= df["close"][i]:
#     #     return True


# class DataCalendarImporter:
#     async def process():
#         folder = PACKAGE_ROOT / "data/calendars"
#         client = IBStubs.client()
#         await client.is_running_async()
#         for underlying in UniverseProvider.contracts_ib():

#             path = folder / f"{underlying.symbol}.parquet"
#             if path.exists():
#                 continue
#             if underlying.symbol != "XINA50":
#                 continue

#             details = await client.get_contract_details(
#                 IBContract(
#                     secType="FUT",
#                     symbol=underlying.symbol,
#                     exchange=underlying.exchange,
#                 ),
#             )
#             contract = details[0].contract
#             df = await client.get_historical_schedule(contract=contract)
#             if df is None:
#                 print(f"{underlying.symbol} does not have calendar information on IB")
#                 continue
#             print(df)
#             df.to_parquet(path)

#     async def _front_contract(instrument_provider, underlying: IBContract) -> FuturesContract:
#         contracts = await instrument_provider.get_future_chain_details(
#                         underlying=underlying,
#                         min_expiry=pd.Timestamp("1970-01-01"),
#                         max_expiry=pd.Timestamp("3000-01-01"),
#         )
#         return contracts[0]

# """
# checks if the market is open
# """
# if ts is None:
#     ts = datetime.now(pytz.utc)

# df = self.trading_hours
# for i in range(len(df)):
#     if ts < df["open"][i]:
#         return False


# return False

# @staticmethod
# def _parse_ib_trading_hours(tradingHours: str, exchange: str) -> pd.DataFrame:
#     """
#     parses trading hours from the contract specs
#     returns a df of trading hours with timestamps localised to the timezone of the exchange
#     """
#     # localize -> non aware to aware
#     # astimezone() -> aware to aware
#     hours = dict(open=[], close=[])
#     for s in tradingHours.split(";"):
#         if s == "":  # string can start with ;
#             continue
#         if "CLOSED" in s:
#             continue
#         tz = pytz.timezone(EXCHANGE_TIMEZONE_MAP[exchange])
#         open_time = tz.localize(
#             datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]), int(s[9:11]), int(s[11:13])),
#             is_dst=None,
#         )
#         close_time = tz.localize(
#             datetime(int(s[14:18]), int(s[18:20]), int(s[20:22]), int(s[23:25]), int(s[25:27])),
#             is_dst=None,
#         )

#         hours["open"].append(open_time.astimezone(pytz.utc))
#         hours["close"].append(close_time.astimezone(pytz.utc))
#     df_hours = pd.DataFrame(hours)
#     return df_hours

# DST handling
#
# https://stackoverflow.com/a/60169568

# @classmethod
# def from_contract(cls, contract: FuturesContract):
#     """
#     localizes all trading hour timestamp within df to UTC
#     returns calendar class
#     """
#     trading_hours = cls._parse_ib_trading_hours(
#         exchange=contract.info["exchange"],
#         tradingHours=contract.info["tradingHours"],
#     )
#     return cls(trading_hours=trading_hours)

# @classmethod
# def from_ib(cls, exchange: str, tradingHours: str):
#     trading_hours = cls._parse_ib_trading_hours(
#         exchange=exchange,
#         tradingHours=tradingHours
#     )
#     return cls(trading_hours=trading_hours)


# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(_main())

# def from_raw_ib(cls, inst):
#     """
#         the above, but for ib instrument from instruments/ib_instruments.json (used by data downloader)
#     """
#     from pytower.
#


# open_time = datetime.datetime.strptime(open_time_str, "%Y%m%d:%H%M")
# open_time = pd.Timestamp(open_time_str, tz=EX_TZ[exchange])
# close_time = datetime.datetime.strptime(close_time_str, "%Y%m%d:%H%M")
# close_time = pd.Timestamp(close_time_str, tz=EX_TZ[exchange])
# close_time = datetime.strptime(open_time_str, "%Y%m%d:%H%M", tzinfo=EX_TZ[exchange])
# init a timestamp with timezone of the contract, then localize to UTC
# timestamp coming in will allways be UTC
# then do all calcs in UTC
#

# return cls()

# do not rename this file - pandas reads a calendar module with the same name
# Calendar.from_ib() -> instruct dataframe and init calendar class from the dataframe
# then we have from_lmax() ->

# then to check if market is open, self.calendar.isOpen()


# openTime -> return open time for the day
#

# data, instruments, roll calendar need to have the same IDs -

# from pysystemtrade
# contract details timezoneId property -> offset
# unused in execution flow, only for reference
# for path in Path(folder).glob("*.json"):

# if str(symbol) not in path.stem:
#     continue
# @classmethod
# def default_start(cls) -> pd.Timestamp:
#     return pd.Timestamp("19700101")

# @classmethod
# def default_end(cls) -> pd.Timestamp:
#     return pd.Timestamp("20400101")

# def _bound_min_error_msg(self, start: pd.Timestamp) -> str:
#     pass

# def _bound_max_error_msg(self, end: pd.Timestamp) -> str:
#     pass

# def _create_breaks(self, calendar: pd.DataFrame) -> Sequence[tuple[pd.Timestamp, datetime.time]]:
#     times = pd.Series(index=self._index, data=pd.NaT)

#     for dayofweek, time in calendar.items():
#         times[times.index.dayofweek == dayofweek] = time

#     assert not (times.isna().any())

#     return list(zip(times.index, times))

# assert list(data.keys()) == list(range(7))
# assert all(len(times) > 0 for times in data.values())
# self._schedule = pd.DataFrame(columns=["day", "open", "close"])
# self._schedule["day"] = data.day.apply(pd.Timestamp)
# self._schedule["open"] = data.day + data.open # .apply(pd.Timedelta)
# self._schedule["close"] = data.close.apply(pd.Timedelta)

# self._schedule["open"] = [
#     day + pd.Timedelta(hours=time.hour, minutes=time.minute)
#     for day, time in zip(data.day, data.open)
# ]

# self._schedule["close"] = [
#     day + pd.Timedelta(hours=time.hour, minutes=time.minute)
#     for day, time in zip(data.day, data.close)
# ]

# open_mask = (now >= self._schedule.open) & (now < self._schedule.close)
# open_sessions = self._schedule[open_mask]
# return None if len(open_sessions) == 0 else open_sessions.iloc[0]

# if next_session_idx == len(self._schedule)

# return self._schedule.iloc[next_session_idx].open

# .index[0]

# now = now.tz_convert(self._timezone)

# open_session_idx = self._get_open_session_idx(now)

# open_mask = (now >= self._schedule.open) & (now < self._schedule.close)
# open_sessions = self._schedule[open_mask]

# if open_sessions.empty:  # market closed

# if len(open_sessions) == 1:  # market open
# open_session = open_sessions
# next_session_idx = open_sessions.index[0] + 1
# else:
# raise RuntimeError(f"Multiple open sessions found for timestamp {now}")

# sessions = self._schedule[self._schedule.day == now.floor("D")]
# if len(sessions) == 1:
#     if now < sessions[0].open:
#         next_session_idx = sessions.index[0]
#     elif now >= sessions[0].close
#         next_session_idx = sessions.index[0] + 1
# else:

#     mask = (now >= sessions.close) & (now < sessions.open.shift(-1))

# next_session_idx = self._get_next_session_idx(now)

# return self._schedule.iloc[next_session_idx].open


# market open
# idx = open_session.name + 1


# return self._schedule.iloc[idx].open

# day = now.floor("D")

# closed_all_day = day not in self._schedule["day"].values

# if closed_all_day:
#     return None

# times = self._schedule[self._schedule.day == day]


# return is_open

# closed_all_day = day not in self._schedule["day"].values

# if closed_all_day:

# days = self._schedule.day.loc[day:]
# for day in days:
#     print(day)
# exit()

# if open, return next session open
# get session of timestamp

# last_day = self._schedule.day.iloc[-1]
# while day <= last_day:


# if day not in self._schedule.day.values:
#     day += pd.Timedelta(days=1)
#     continue

# times = self._schedule[self._schedule.day == day]

# now_time = now.time()

# is_open = ((now_time >= times.open) & (now_time < times.close)).any()
# print(is_open)
# exit()

# open_mask = (now_time >= times.open) & (now_time < times.close)
# close_time = times[open_mask].close.iloc[0]
# return pd.Timestamp(day).replace(hour=close_time.hour, minute=close_time.minute)
# print(close_time)
# exit()
# return times[open_mask].close

# if matched:


# return times.open

# exit()


# times = self._schedule.loc[current_day]

# closed_all_day = times.isna().all()

# if closed_all_day or now >= times.open_time:
#     return self._schedule.loc[next_day].open_time
# else:
#     return times.open_time
# def _get_open_session_idx(self, now: pd.Timestamp) -> int:
#     open_mask = (now >= self._schedule.open) & (now < self._schedule.close)
#     open_sessions = self._schedule[open_mask]
#     if len(open_sessions) == 0:
#         return -1
#     assert len(open_sessions) == 1
#     return open_sessions.index[0]

# def _get_next_session_idx(self, now: pd.Timestamp) -> int:
#     assert self._get_open_session_idx(now) is None
#     sessions = self._schedule.day[now.floor("D")]
#     idx = sessions.iloc[-1].index + 1

#     if idx == len(self._schedule): # no data
#         return None
#     return self._schedule.iloc[idx]

# closed_all_day = day not in self._schedule["day"].values

# if closed_all_day:
#     return False

# times = self._schedule[self._schedule.day == day]
# return is_open
# now = now.tz_convert(self._timezone)

# times = self._schedule.loc[now.floor("D")]

# has_break = times.break_start_time is not None and times.break_end_time is not None

# if has_break and now < times.break_start_time:
#     close_time = times.break_start_time
# else:
#     close_time = times.close_time

# return close_time - now
# df.dropna(axis=1, inplace=True)
# print(df)
# for dayofweek in range(7):

#     time_pairs = data.get(dayofweek)
#     if time_pairs is None:
#         continue

#     for time_pair in time_pairs:
#         open, close = time_pair


# for dayofweek, time_pairs in data.items():

#     closed_all_day = time_pairs == [(datetime.time(0, 0), datetime.time(0, 0))]
#     no_break = len(time_pairs) == 1
#     has_break = len(time_pairs) == 2

#     if closed_all_day:
#         calendar.loc[len(calendar)] = (
#             None,
#             None,
#             None,
#             None,
#         )
#     elif no_break:
#         calendar.loc[len(calendar)] = (
#             time_pairs[0][0],
#             None,
#             None,
#             time_pairs[0][1],
#         )
#     elif has_break:
#         calendar.loc[len(calendar)] = (
#             time_pairs[0][0],
#             time_pairs[0][1],
#             time_pairs[1][0],
#             time_pairs[1][1],
#         )
#     else:
#         print(time_pairs)
#         raise RuntimeError("Only one break per day supported")

# assert len(calendar) == 7

# parse schedule
