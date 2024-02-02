from __future__ import annotations

import datetime
import json

import pandas as pd
import pytz

from nautilus_trader.model.identifiers import Symbol
from pyfutures import PACKAGE_ROOT
from pyfutures.schedule.schedule import MarketSchedule
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.schedule.factory import MarketScheduleFactory
        
DAY_NAME_TO_INT = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}
# https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
# DST offset - confirmation that when pytz localizes a time, the UTC offset also includes the DST offset.

# Exchange -> pytz timezone mapping
# some of these exchanges are not in the city of the timezone
# when the exchange is in the country of the timezone, the trading hours are localized to UTC being DST aware
# https://www.tradinghours.com/markets/jpx

TIMEZONE_FOR_EXCHANGE = {
    # US
    "CFE": pytz.timezone("America/Chicago"),  # Cboe
    "CME": pytz.timezone("America/Chicago"),
    "CBOT": pytz.timezone("America/Chicago"),
    "COMEX": pytz.timezone("America/Chicago"),  # CME - US/Eastern
    "NYMEX": pytz.timezone("America/Chicago"),  # CME Nymex
    "NYBOT": pytz.timezone("America/New_York"),  # ICE U.S Futures - US/Eastern
    "NYSELIFFE": pytz.timezone("America/New_York"),  # NYSE
    "CDE": pytz.timezone("America/Montreal"),
    "MONEP": pytz.timezone("Europe/Paris"),
    # Europe
    "EUREX": pytz.timezone("Europe/Berlin"),  # Frankfurt, Germany
    "IDEM": pytz.timezone("Europe/Rome"),  # Borsa Italiana - not found
    "FTA": pytz.timezone("Europe/Amsterdam"),
    "ENDEX": pytz.timezone("Europe/Amsterdam"),
    "MEFFRV": pytz.timezone("Europe/Madrid"),
    "ICEEU": pytz.timezone("Europe/London"),
    "ICEEUSOFT": pytz.timezone("Europe/London"),
    # ASIA
    "HKFE": pytz.timezone("Asia/Hong_Kong"),  # HKEX
    "SGX": pytz.timezone("Asia/Singapore"),
    "KSE": pytz.timezone("Asia/Seoul"),  # KRX
    "OSE|JPN": pytz.timezone("Asia/Tokyo"),  # JPX
}


class MarketScheduleFactory:
    FOLDER = PACKAGE_ROOT / "tests/adapters/interactive_brokers/demo/import_instrument_json"

    @classmethod
    def from_symbol(cls, symbol: Symbol, open_offset: pd.Timedelta = None) -> MarketSchedule | None:
        calendar = cls.from_liquid_hours(symbol, open_offset=open_offset)
        if calendar is None:
            calendar = cls.from_market_hours(symbol, open_offset=open_offset)
            if calendar is None:
                return None

        return calendar

    @classmethod
    def from_market_hours(
        cls,
        symbol: Symbol,
        open_offset: pd.Timedelta = None,
    ) -> MarketSchedule | None:
        return cls._from_key(symbol=symbol, key="Trading Hours", open_offset=open_offset)

    @classmethod
    def from_liquid_hours(
        cls,
        symbol: Symbol,
        open_offset: pd.Timedelta = None,
    ) -> MarketSchedule | None:
        return cls._from_key(symbol=symbol, key="Liquid Trading Hours", open_offset=open_offset)

    @classmethod
    def _from_key(
        cls,
        symbol: Symbol,
        key: str,
        open_offset: pd.Timedelta = None,
    ) -> MarketSchedule | None:
        path = cls.FOLDER / f"{symbol}.json"

        assert path.exists()

        # Open the JSON file for reading
        with open(path) as json_file:
            json_data = json.load(json_file)

        # parse hours
        hours = json_data.get(key)

        if hours is None:
            return None

        hours = cls._parse_hours(hours, open_offset=open_offset)

        # parse timezone
        exchange = json_data["Exchange"].replace(".", "_")
        if "," in exchange:
            exchange = exchange.split(",")[0]
        timezone = TIMEZONE_FOR_EXCHANGE[exchange]

        return MarketSchedule(
            name=str(symbol),
            data=hours,
            timezone=timezone,
        )

    @staticmethod
    def _parse_hours(value: str, open_offset: pd.Timedelta = None) -> pd.DataFrame:
        df = pd.DataFrame(columns=["dayofweek", "open", "close"])

        for day, time_strs in value.items():
            for times in time_strs:
                parts = times.split("-")

                close_time = pd.to_datetime(parts[1], format="%H:%M").time()

                open_time = pd.to_datetime(parts[0], format="%H:%M").time()

                if open_time == datetime.time(0, 0) and close_time == datetime.time(0, 0):
                    continue

                if open_offset is not None:
                    open_time = datetime.time(open_time.hour + 1, open_time.minute)

                    if open_time >= close_time:
                        continue

                dayofweek: int = DAY_NAME_TO_INT[day]

                df.loc[len(df)] = (dayofweek, open_time, close_time)

        return df.sort_values(by=["dayofweek", "open"])

    @staticmethod
    def market_hour_ranges_utc():
        df = pd.DataFrame(columns=["name", "open", "close"])

        universe = IBTestProviderStubs.universe_dataframe()
        for symbol in universe.symbol:
            calendar = MarketScheduleFactory.from_market_hours(symbol, open_offset=1)

            if calendar is None:
                continue

            ndf = calendar.to_weekly_calendar_utc()
            ndf["name"] = str(symbol)
            df = pd.concat([df, ndf], ignore_index=True)

        df.dayofweek = df.dayofweek.astype(int)
        return df.sort_values("open")

    @classmethod
    def create_calendar_range_plot(cls):
        # Plotly
        # https://plotly.com/python/gantt/
        # https://plotly.com/python-api-reference/generated/plotly.express.timeline.html
        # https://plotly.com/python/discrete-color/#color-sequences-in-plotly-express

        import plotly.express as px

        df = cls.market_hour_ranges_utc()

        fig = px.timeline(
            df,
            x_start="open",
            x_end="close",
            y="name",
            text="name",
            color_discrete_sequence=["tan"],
            width=2560,
            height=1440,
        )

        fig.show()
        fig.write_image("/Users/g1/Desktop/plotly_plot.png")

    @staticmethod
    def find_incorrect_universe_hours():
        
        # factory = MarketScheduleFactory().from_symbol()

        factory = MarketScheduleFactory()

        df = factory.market_hour_ranges_utc()

        universe = IBTestProviderStubs.universe_dataframe()

        for row in universe.itertuples():
            symbol = row.symbol
            open_time = row.open
            close_time = row.close

            expected = df[(df.name == symbol) & (df.dayofweek == 0)]

            assert not expected.empty
            expected.open = expected.open.apply(lambda x: x.time())
            expected.close = expected.close.apply(lambda x: x.time())

            # print(expected)

            matched = (
                (expected.open.values == open_time) & (expected.close.values == close_time)
            ).any()

            if not matched:
                print(expected)
                print(row.tradingClass, symbol, open_time, close_time)
                print("\n")

    @staticmethod
    def find_weekly_hours_not_the_same():
        # find hours that are not the same each weeky
        

        factory = MarketScheduleFactory()

        df = factory.market_hour_ranges_utc()
        universe = IBTestProviderStubs.universe_dataframe()

        for row in universe.itertuples():
            symbol = row.symbol
            open_time = row.open
            close_time = row.close

            expected = df[(df.name == symbol)]
            expected.open = expected.open.apply(lambda x: x.time())
            expected.close = expected.close.apply(lambda x: x.time())

            for dayofweek in range(1, 5):
                ndf = expected[expected.dayofweek == dayofweek]

                matched = ((ndf.open.values == open_time) & (ndf.close.values == close_time)).any()
                if not matched:
                    print(dayofweek, symbol)

    # @staticmethod
    # def _parse_hours(value: str) -> dict:

    #     data = {}
    #     for day, time_strs in value.items():
    #         for times in time_strs:
    #             parts = times.split("-")
    #             open_time = pd.to_datetime(parts[0], format='%H:%M').time()
    #             close_time = pd.to_datetime(parts[1], format='%H:%M').time()
    #             if open_time == datetime.time(0, 0) and close_time == datetime.time(0, 0):
    #                 continue

    #             data.setdefault(DAY_NAME_TO_INT[day], []).append((open_time, close_time))
    #     data = {k: data[k] for k in sorted(data)}  # sort dict
    #     return data

    # Non-universe exchange timezones
    # "MEXDER": pytz.timezone("America/Mexico_City"),
    # "ICEUS": pytz.timezone("America/New_York"),  # ICE U.S Futures - US/Eastern
    # "SOFFEX": None,  # there are no SOFFEX future instruments atm
    # "SNFE": pytz.timezone("Australia/Sydney"),  # ASX
    # "BELFOX": pytz.timezone("Europe/Brussels"),  # Euronext Brussels
    # BME - https://en.wikipedia.org/wiki/Bolsas_y_Mercados_Espa%C3%B1oles
    # Nasdaq OMX -  https://en.wikipedia.org/wiki/Nasdaq_Nordic
    # https://www.nasdaqomxnordic.com/tradinghours
    # "MATIF": pytz.timezone("Europe/Paris"),
    # "SMFE": pytz.timezone("America/Chicago"),
    # "OMS": pytz.timezone("Europe/Stockholm"),
    # India has only 1 timezone: IST - any city inside India will work
    # "NSE": pytz.timezone("Asia/Kolkata"), #Mumbai, India - no pytz timezone, closest is Asia/Kolkata
    # "IPE": pytz.timezone("Europe/London"),  # International Petroleum Exchange
    # "LMEOTC": pytz.timezone("Europe/London"),


# TIME_DIFFERENCE_FOR_TIMEZONE = {
#     "US/Eastern": 5,
#     "MET": -1,
#     "Japan": -9,
#     "US/Central": 6,
#     "GB-Eire": 0,
#     "Hongkong": -8,
#     # "CST (Central Standard Time)": 6,
#     # "MET (Middle Europe Time)": -1,
#     # "EST (Eastern Standard Time)": 5,
#     # "JST (Japan Standard Time)": -9,
#     # "EST": 5,
#     # "JST": -9,
#     # "Australia/NSW": -11,
#     # "": 0,
# }
