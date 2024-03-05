from pathlib import Path

import pandas as pd
import pytest
from ibapi.contract import Contract as IBContract
from pytower import PACKAGE_ROOT
from pyfutures.adapter.client.objects import ClientException
from pyfutures.adapter.enums import WhatToShow
from pyfutures.tests.test_kit import IBTestProviderStubs

from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

init_logging(level_stdout=LogLevel.DEBUG)


@pytest.mark.asyncio()
async def test_request_head_timestamp_contfut(client):
    contract = IBContract()

    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "CONTFUT"

    # await client.connect()

    # contract = await client.request_front_contract(contract)

    contract = IBContract()

    contract.tradingClass = "FMEU"
    contract.symbol = "M7EU"
    contract.exchange = "EUREX"
    contract.secType = "CONTFUT"

    await client.connect()

    details = await client.request_contract_details(contract)
    print(len(details))


@pytest.mark.asyncio()
async def test_request_head_timestamp_universe(client):
    await client.connect()
    rows = IBTestProviderStubs.universe_rows()

    for row in rows:
        contract = await client.request_front_contract(row.contract)

        timestamp = await client.request_head_timestamp(
            contract=row.contract,
            what_to_show=WhatToShow.BID,
        )

        if timestamp is None:
            print(
                f"No head timestamp for {row.instrument_id} {contract.lastTradeDateOrContractMonth}",
            )
            continue
        # print(
        #     f"Head timestamp found: {timestamp}  {contract.symbol} {contract.exchange} {contract.lastTradeDateOrContractMonth} {contract.conId}",
        # )


@pytest.mark.asyncio()
async def test_request_front_contract_universe(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()

    universe = IBTestProviderStubs.universe_dataframe()
    timezones = []

    for row in universe.itertuples():
        try:
            details = await client.request_front_contract_details(row.contract)

            assert type(details) is IBContractDetails
            timezones.append(details.timeZoneId)
            print(row.trading_class, details.timeZoneId)

        except ClientException as e:
            if e.code == 200:
                timezones.append("None")
                print(row.trading_class, details.timeZoneId)
            else:
                raise e


@pytest.mark.asyncio()
async def test_request_front_contract_universe_fix(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()

    contract = IBContract()
    contract.symbol = "XT"
    contract.tradingClass = "XT"
    contract.exchange = "SNFE"
    contract.secType = "FUT"
    contract.includeExpired = False

    try:
        contract = await client.request_front_contract(contract)
        assert type(contract) is Contract
    except ClientException as e:
        if e.code == 200:
            print(f"{row.trading_class}")
        else:
            raise e


def test_historic_schedules_with_sessions_out_of_day():
    """
    find instruments that have sessions where the start and end date is NOT within the same day
    """
    schedules_dir = Path(
        "/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/schedules"
    )
    universe = IBTestProviderStubs.universe_dataframe()

    for row in universe.itertuples():
        if row.trading_class == "COIL_Z":
            continue

        path = schedules_dir / f"{row.trading_class}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if not (df.start.dt.date == df.end.dt.date).all():
                print(row.trading_class)
                # print(df)


def find_minimum_day_of_month_within_range(
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    dayname: str,  # Friday, Tuesday etc
    dayofmonth: int,  # 1, 2, 3, 4 etc
):
    # find minimum x day of each month within a date range
    import pandas as pd

    df = pd.DataFrame({"date": pd.date_range(start=start, end=end)})
    days = df[df["date"].dt.day_name() == dayname]

    data = {}
    for date in days.date:
        key = f"{date.year}{date.month}"
        if data.get(key) is None:
            data[key] = []

        data[key].append(date)

    filtered = []
    for key, values in data.items():
        filtered.append(values[dayofmonth - 1])

    return pd.Series(filtered).dt.day.min()

def test_find_missing_calendars(self):
    missing = set()

    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        path = MarketCalendarFactory.FOLDER / f"{instrument_id.symbol}.json"

        assert path.exists()

        # Open the JSON file for reading
        with open(path) as json_file:
            data = json.load(json_file)

        if data.get("Trading Hours") is None and data.get("Liquid Trading Hours") is None:
            missing.add(instrument_id)

    for item in missing:
        print(item)
            
def test_from_symbol_universe(self):
        missing = set()
        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            result = MarketCalendarFactory.from_symbol(instrument_id.symbol)
            if result is None:
                missing.add(instrument_id.symbol)
        for symbol in missing:
            print(symbol)
    
def test_from_market_hours_universe(self):
    missing = set()
    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        result = MarketCalendarFactory.from_market_hours(instrument_id.symbol)
        if result is False:
            missing.add(instrument_id)

    for item in missing:
        print(item)
            
def test_multiple_exchanges_universe():
    import json

    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        folder = PACKAGE_ROOT / "data/ib/instruments/info_universe_json"
        path = folder / f"{instrument_id.symbol}.json"
        with open(path) as json_file:
            json_data = json.load(json_file)
            if "," in json_data["Exchange"]:
                print(instrument_id, json_data["Exchange"])

def test_from_liquid_hours_universe(self):
    missing = set()
    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        result = MarketCalendarFactory.from_liquid_hours(instrument_id.symbol)
        if result is False:
            missing.add(instrument_id)

    for instrument_id in missing:
        print(instrument_id)

def test_exchange_timezone_map(self):
    exchanges = list(TIMEZONE_FOR_EXCHANGE.keys())
    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        # TIMEZONE_FOR_EXCHANGE.get(instrument_id.venue.value)

        exchange = instrument_id.venue.value
        timezone = TIMEZONE_FOR_EXCHANGE.get(exchange)
        assert timezone is not None

        if exchange in exchanges:
            exchanges.remove(exchange)

    print(f"Unrequired: {exchanges}")
            
if __name__ == "__main__":
    test_historic_schedules_with_sessions_out_of_day()

# def test_find_problem_files():
#     """
#     find trading_classes where the files do have the hold cycle in every year
#     """
#     universe = IBTestProviderStubs.universe_dataframe()
#     data_folder = Path("/Users/g1/Desktop/portara data george/DAY")
#     for row in universe.itertuples():

#         data_dir = (data_folder / row.data_symbol)
#         paths = list(data_dir.glob("*.txt")) \
#                     + list(data_dir.glob("*.b01")) \
#                     + list(data_dir.glob("*.bd"))

#         paths = list(sorted(paths))
#         assert len(paths) > 0

#         start_month = ContractMonth(row.data_start)
#         end_month = ContractMonth(row.data_end)
#         required_months = []

#         cycle = RollCycle.from_str(row.hold_cycle)

#         while True:
#             required_months.append(start_month)
#             start_month = cycle.next_month(start_month)
#             if start_month >= end_month:
#                 break

#         stems = [
#             x.stem[-5:] for x in paths
#         ]
#         for month in required_months:
#             if month.value in stems:
#                 continue
#             print(row.trading_class, month.value)
