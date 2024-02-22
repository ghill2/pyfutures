from datetime import time

import pandas as pd
import pytz
import pickle
from pyfutures.continuous.schedule import MarketSchedule
import datetime


class TestMarketSchedule:
    def setup(self):
        self.data = pd.DataFrame(
            columns=["dayofweek", "open", "close"],
        )

        # Tuesday
        self.data.loc[len(self.data)] = {"dayofweek": 1, "open": time(8, 30), "close": time(16, 0)}
        self.data.loc[len(self.data)] = {"dayofweek": 1, "open": time(16, 30), "close": time(17, 0)}
        self.data.loc[len(self.data)] = {
            "dayofweek": 1,
            "open": time(20, 15),
            "close": time(23, 59),
        }

        # Wednesday
        self.data.loc[len(self.data)] = {"dayofweek": 2, "open": time(8, 30), "close": time(16, 0)}

        # Thursday
        self.data.loc[len(self.data)] = {"dayofweek": 3, "open": time(8, 30), "close": time(16, 0)}

        # Saturday
        self.data.loc[len(self.data)] = {"dayofweek": 5, "open": time(17, 0), "close": time(23, 59)}

    def test_schedule_previous_trading_day(self):
        calendar = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)

        now_day = datetime.date(2024, 1, 27)  # Saturday
        expected = datetime.date(2024, 1, 25)  # Thursday
        assert calendar.previous_trading_day(date=now_day, offset=-1) == expected

        now_day = datetime.date(2024, 1, 23)  # Tuesday
        expected = datetime.date(2024, 1, 20)  # Saturday
        assert calendar.previous_trading_day(date=now_day, offset=-1) == expected

        now_day = datetime.date(2024, 1, 25)  # Thursday
        expected = datetime.date(2024, 1, 24)  # Wedsnesday
        assert calendar.previous_trading_day(date=now_day, offset=-1) == expected

        now_day = datetime.date(2024, 1, 26)  # Friday
        expected = datetime.date(2024, 1, 25)  # Thursday
        assert calendar.previous_trading_day(date=now_day, offset=-1) == expected

    def test_is_open_utc(self):
        calendar = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)

        assert not calendar.is_open(pd.Timestamp("1980-01-01 08:29:00", tz="UTC"))  # Tuesday
        assert calendar.is_open(pd.Timestamp("1980-01-01 08:30:00", tz="UTC"))  # Tuesday

        assert not calendar.is_open(pd.Timestamp("1980-01-04 08:30:00", tz="UTC"))  # Friday
        assert calendar.is_open(pd.Timestamp("1980-01-01 15:59:00", tz="UTC"))  # Tuesday
        assert not calendar.is_open(pd.Timestamp("1980-01-01 16:00:00", tz="UTC"))  # Tuesday

        assert not calendar.is_open(pd.Timestamp("1980-01-01 16:29:00", tz="UTC"))  # Tuesday
        assert calendar.is_open(pd.Timestamp("1980-01-01 16:30:00", tz="UTC"))  # Tuesday

        assert calendar.is_open(pd.Timestamp("1980-01-01 16:59:00", tz="UTC"))  # Tuesday
        assert not calendar.is_open(pd.Timestamp("1980-01-01 17:00:00", tz="UTC"))  # Tuesday

        assert calendar.is_open(pd.Timestamp("1980-01-01 20:15:00", tz="UTC"))  # Tuesday
        assert not calendar.is_open(pd.Timestamp("1980-01-01 20:14:00", tz="UTC"))  # Tuesday

    def test_is_open_with_timezone(self):
        calendar = MarketSchedule(
            name="test",
            data=self.data,
            timezone=pytz.timezone("America/Chicago"),
        )

        assert calendar.is_open(pd.Timestamp("2023-10-28 02:00:00", tz="UTC"))  # Saturday

        assert not calendar.is_open(pd.Timestamp("1980-01-01 14:29:00", tz="UTC"))  # Tuesday
        assert calendar.is_open(pd.Timestamp("1980-01-01 14:30:00", tz="UTC"))  # Tuesday

        assert calendar.is_open(pd.Timestamp("1980-01-01 21:59:00", tz="UTC"))  # Tuesday
        assert not calendar.is_open(pd.Timestamp("1980-01-01 22:00:00", tz="UTC"))  # Tuesday

    def test_next_open_utc(self):
        calendar = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)

        # Thursday > Saturday
        assert calendar.next_open(pd.Timestamp("1980-01-03 08:30", tz="UTC")) == pd.Timestamp(
            "1980-01-05 17:00",
            tz="UTC",
        )

        # Tuesday before first session
        assert calendar.next_open(pd.Timestamp("1980-01-01 08:29", tz="UTC")) == pd.Timestamp(
            "1980-01-01 08:30",
            tz="UTC",
        )

        # Tuesday in first session
        assert calendar.next_open(pd.Timestamp("1980-01-01 08:30", tz="UTC")) == pd.Timestamp(
            "1980-01-01 16:30",
            tz="UTC",
        )

        # Tuesday before second session
        assert calendar.next_open(pd.Timestamp("1980-01-01 16:29", tz="UTC")) == pd.Timestamp(
            "1980-01-01 16:30",
            tz="UTC",
        )

        # Tuesday in second session
        assert calendar.next_open(pd.Timestamp("1980-01-01 16:30", tz="UTC")) == pd.Timestamp(
            "1980-01-01 20:15",
            tz="UTC",
        )

        # Tuesday before third session
        assert calendar.next_open(pd.Timestamp("1980-01-01 20:14", tz="UTC")) == pd.Timestamp(
            "1980-01-01 20:15",
            tz="UTC",
        )

        # Tuesday in third session
        assert calendar.next_open(pd.Timestamp("1980-01-01 20:15", tz="UTC")) == pd.Timestamp(
            "1980-01-02 08:30",
            tz="UTC",
        )

        # Thursday > Thursday
        assert calendar.next_open(pd.Timestamp("1980-01-03 08:29", tz="UTC")) == pd.Timestamp(
            "1980-01-03 08:30",
            tz="UTC",
        )

        # Friday (closed) > Saturday
        assert calendar.next_open(pd.Timestamp("1980-01-04 08:29", tz="UTC")) == pd.Timestamp(
            "1980-01-05 17:00",
            tz="UTC",
        )

    def test_next_open_with_timezone(self):
        pass

    def test_time_until_close(self):
        calendar = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)

        # tuesday
        assert calendar.time_until_close(
            pd.Timestamp("1980-01-01 15:59:00", tz="UTC"),
        ) == pd.Timedelta(minutes=1)
        assert calendar.time_until_close(pd.Timestamp("1980-01-01 16:00:00", tz="UTC")) is None

        assert calendar.time_until_close(pd.Timestamp("1980-01-01 16:29:00", tz="UTC")) is None
        assert calendar.time_until_close(
            pd.Timestamp("1980-01-01 16:30:00", tz="UTC"),
        ) == pd.Timedelta(minutes=30)

        # friday (closed)
        assert calendar.time_until_close(pd.Timestamp("1980-01-04 16:00:00", tz="UTC")) is None

        # weds
        assert calendar.time_until_close(
            pd.Timestamp("1980-01-02 08:30:00", tz="UTC"),
        ) == pd.Timedelta("0 days 07:30:00")

    def test_equality(self):
        schedule = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)
        assert schedule == schedule

    def test_schedule_pickle(self):
        # Arrange
        schedule = MarketSchedule(name="test", data=self.data, timezone=pytz.UTC)

        # Act
        pickled = pickle.dumps(schedule)
        unpickled = pickle.loads(pickled)  # noqa S301 (pickle is safe here)

        # Assert
        assert unpickled == schedule

    # def test_ib_pandas_tz(self):
    #     for tz in [
    #         "CST (Central Standard Time)",
    #         "MET (Middle Europe Time)",
    #         "EST (Eastern Standard Time)",
    #         "JST (Japan Standard Time)",
    #         "US/Eastern",
    #         "MET",
    #         "EST",
    #         "JST",
    #         "Japan",
    #         "US/Central",
    #         "GB-Eire",
    #         "Hongkong",
    #         "Australia/NSW",
    #     ]:
    #         pd.Timestamp("2017-01-12", tz=tz)

    # def parse_trading_hours(self):
    #     data_calendar = DataCalendar()
    #     # SGX|ZADS
    #     df_hours = data_calendar._parse_ib_trading_hours(
    #         # timeZoneId="MET",
    #         exchange="SGX",
    #         tradingHours="20231016:0855-20231016:1815;20231017:0855-20231017:1815;20231018:0855-20231018:1815;20231019:0855-20231019:1815;20231020:0855-20231020:1815",
    #     )
    #     print(df_hours)

    # def all_trading_hours_website(self):
    #     """
    #     manual test:
    #     """
    #     import webbrowser

    #     urls = []
    #     data_calendar = DataCalendar()

    #     print("SNFE|ASX - IR")
    #     urls.append("https://www.tradinghours.com/markets/asx")
    #     df_hours = data_calendar._parse_ib_trading_hours(
    #         exchange="SNFE",
    #         tradingHours="20231016:0828-20231016:1630;20231016:1708-20231017:0700;20231017:0828-20231017:1630;20231017:1708-20231018:0700;20231018:0828-20231018:1630;20231018:1708-20231019:0700;20231019:0828-20231019:1630;20231019:1708-20231020:0700;20231020:0828-20231020:1630;20231020:1708-20231021:0700",
    #     )
    #     # print(df_hours)

    #     print("SGX|ZADS")
    #     urls.append("https://www.tradinghours.com/markets/sgx")
    #     df_hours = data_calendar._parse_ib_trading_hours(
    #         exchange="SGX",
    #         tradingHours="20231016:0855-20231016:1815;20231017:0855-20231017:1815;20231018:0855-20231018:1815;20231019:0855-20231019:1815;20231020:0855-20231020:1815",
    #     )
    #     print(df_hours)

    #     print("KSE|1BK")
    #     urls.append("https://www.tradinghours.com/markets/krx")
    #     df_hours = data_calendar._parse_ib_trading_hours(
    #         exchange="KSE",
    #         tradingHours="20231016:0900-20231016:1545;20231017:0900-20231017:1545;20231018:0900-20231018:1545;20231019:0900-20231019:1545;20231020:0900-20231020:1545",
    #     )
    #     print(df_hours)

    #     for url in urls:
    #         webbrowser.open(url)

    # def print_tz(self):
    #     import pytz
    #     import pprint

    #     # Get a list of all available timezones
    #     timezones = pytz.all_timezones
    #     pprint.pprint(timezones)

    # def is_market_open(self):
    #     data_calendar = DataCalendar()
    #     print("KSE|1BK")
    #     df_hours = data_calendar._parse_ib_trading_hours(
    #         exchange="KSE",
    #         tradingHours="20231016:0900-20231016:1545;20231017:0900-20231017:1545;20231018:0900-20231018:1545;20231019:0900-20231019:1545;20231020:0900-20231020:1545",
    #     )
    #     data_calendar.trading_hours = df_hours
    #     status = data_calendar.is_market_open()

    # def test_all_contracts(self):
    #     from pytower.instruments.universe.provider import UniverseProvider

    #     instrument_provider = PytowerInstrumentProvider()
    #     contracts = instrument_provider.list_all()
    #     for contract in contracts:
    #         if str(contract.id.venue) == "IB":
    #             # print(contract.info["exchange"], contract.raw_symbol)
    #             # print(contract.info["tradingHours"])
    #             data_calendar = DataCalendar.from_ib(contract=contract)
    #             if data_calendar.is_market_open():
    #                 print(str(contract.id.venue), "OPEN")
