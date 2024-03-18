import logging
from datetime import timezone
from pathlib import Path

import pandas as pd
import pytz

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.client.historic import InteractiveBrokersHistoricClient
from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.unit.client.stubs import ClientStubs


def _merge_dataframe(bid_df: pd.DataFrame, ask_df: pd.DataFrame) -> pd.DataFrame:
    bid_df.sort_values(by="timestamp", inplace=True)
    ask_df.sort_values(by="timestamp", inplace=True)

    df = pd.merge(bid_df, ask_df, on="timestamp", how="inner", suffixes=("_bid", "_ask"))
    df.drop(
        [
            "date_ask",
            "date_bid",
            "open_bid",
            "open_ask",
            "high_bid",
            "high_ask",
            "low_bid",
            "low_ask",
            "volume_bid",
            "volume_ask",
            "barCount_bid",
            "barCount_ask",
            "wap_bid",
            "wap_ask",
        ],
        axis=1,
        inplace=True,
    )
    df.rename(
        {
            "close_bid": "bid",
            "close_ask": "ask",
        },
        axis=1,
        inplace=True,
    )
    assert list(df.columns) == ["timestamp", "bid", "ask"]
    df = df[(df.bid != 0.0) & (df.ask != 0.0)]
    # df.timestamp = df.timestamp.dt.tz_localize("UTC")
    return df


def find_missing_sessions(row):
    path = SPREAD_FOLDER / f"{row.uname}_BID.parquet"
    df = pd.read_parquet(path)

    end_time = pd.Timestamp.utcnow().floor(pd.Timedelta(days=1)) - pd.Timedelta(days=1)
    start_time = end_time - pd.Timedelta(days=128)
    schedule = row.liquid_schedule
    open_times = schedule.to_open_range(
        start_date=start_time,
        end_date=end_time,
    )

    for i, open_time in enumerate(open_times):
        # open_time = open_times[3]
        end = open_time + pd.Timedelta(hours=1)
        start = open_time
        assert start.tzinfo == timezone.utc
        assert end.tzinfo == timezone.utc
        for t in df.timestamp:
            assert t.tzinfo == pytz.UTC

        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.width",
            None,
        ):
            df = df[(df.timestamp >= start) & (df.timestamp < end)]
            print(df)
            if df.empty:
                print(f"No data for session {row.uname}. {start} > {end}")
            print(open_time)
            exit()


def get_spread_value(row):
    bid_df = pd.read_parquet(SPREAD_FOLDER / f"{row.uname}_BID.parquet")

    ask_df = pd.read_parquet(SPREAD_FOLDER / f"{row.uname}_ASK.parquet")

    df = _merge_dataframe(bid_df, ask_df)

    schedule = row.liquid_schedule

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        df["local"] = df.timestamp.dt.tz_convert("MET")
        df["dayofweek"] = df.local.dt.dayofweek

        # previous_len = len(df)
        # mask = schedule.is_open_list(list(df.timestamp))
        # mask = df.timestamp.apply(schedule.is_open)
        # df = df[mask]
        # assert not df.empty

        df = df.iloc[-3000:]
        print(df)

        # Group by date and hour
        grouped = df.groupby(pd.Grouper(key="timestamp", freq="H"))

        values = []
        for group_key, df in grouped:
            if df.empty:
                continue

            random_row = df.sample().iloc[0]
            average_spread = random_row.ask - random_row.bid
            values.append(average_spread)

        average_spread: float = pd.Series(values).mean()
        i = 7
        if average_spread == 0.0:
            print(f"{row.trading_class}: {average_spread:.{i}f} failed")
        else:
            print(f"{row.trading_class}: {average_spread:.{i}f}")


async def find_spread_values(row):
    schedule = row.liquid_schedule
    client: InteractiveBrokersClient = ClientStubs.client(
        request_timeout_seconds=60 * 10,
        override_timeout=False,
        api_log_level=logging.ERROR,
    )

    end_time = pd.Timestamp.utcnow().floor(pd.Timedelta(days=1)) - pd.Timedelta(days=1)
    start_time = end_time - pd.Timedelta(days=128)
    open_times = schedule.to_open_range(
        start_date=start_time,
        end_date=end_time,
    )
    open_times = [t for t in open_times if t.dayofweek == 2]
    open_time = open_times[::-1][0]

    await client.connect()
    await client.request_market_data_type(4)

    historic = InteractiveBrokersHistoricClient(
        client=client,
    )
    seconds_in_hour: int = 60 * 60
    seconds_in_day: int = seconds_in_hour * 24

    df = await historic.request_bars(
        contract=row.contract_cont,
        bar_size=BarSize._5_SECOND,
        what_to_show=WhatToShow.BID,
        start_time=open_time.floor(pd.Timedelta(days=1)),
        end_time=open_time.ceil(pd.Timedelta(days=1)),
        cache=None,
        as_dataframe=True,
        delay=0.5,
    )

    # df = await historic.request_quotes(
    #     contract=row.contract_cont,
    #     # start_time=open_time.floor(pd.Timedelta(days=1)),
    #     start_time=open_time.floor(pd.Timedelta(days=1)),
    #     end_time=open_time.ceil(pd.Timedelta(days=1)),
    #     cache=None,
    #     as_dataframe=True,
    #     delay=0.5,
    # )

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        if df.empty:
            print("empty")
            return

        df["local"] = df.timestamp.dt.tz_convert(row.timezone)
        df["dayofweek"] = df.local.dt.dayofweek
        path = Path.home() / "Desktop/DC.parquet"
        print(open_time, open_time.dayofweek)
        print(df)
        df.to_parquet(path, index=False)


if __name__ == "__main__":
    """
    DC bars
    15:30 > 16:00
    16:00 > 22:00
    DC quotes
    # 15:29:55+01:00 > 20:54:58+01:00
    
    KE
    
    """
    rows = IBTestProviderStubs.universe_rows(
        filter=["DC"],
    )
    # rows = [r for r in rows if r.uname == "FTI"]
    # get_spread_value(rows[0])
    find_missing_sessions(rows[0])
    # for row in rows:
    #     asyncio.get_event_loop().run_until_complete(find_missing_sessions(row))

    # """
    # 2024-02-28 21:00:00+00:00
    # 2024-02-28 23:45:00+00:00
    # error with schedule:

    # FTI:
    # FCE:
    # MFA:
    # MFC:
    # CN:
    # TWN:
    # SGP:
    # IU:
    # M6A:
    # UC:
    # FEF:
    # """
    # results = joblib.Parallel(n_jobs=-1, backend="loky")(joblib.delayed(get_spread_value)(row) for row in rows)

    # for i, row in enumerate(rows):
    # 0     2023-11-01 18:19:00+00:00  1698862740  380.75  380.75  380.00  380.75     -1  -100        -1

    # load bars as dataframe

    # filter bars in liquid hours

    # return dataframe for each hour

    # return a random row in the dataframe for each dataframe

    # average all random rows


# def get_spread_value(row):
#     path = SPREAD_FOLDER / f"{row.trading_class}.parquet"

#     df = pd.read_parquet(path)

#     # print(len(df))
#     # TODO: why is this the same length after filtering by liquid hours
#     mask = df.timestamp.apply(row.liquid_schedule.is_open)
#     df = df[mask]

#     # print(len(df))
#     # Group by date and hour
#     grouped = df.groupby(pd.Grouper(key="timestamp", freq="H"))

#     """
#     BID_ASK:
#         open: Time average bid
#         high: Max Ask
#         Low: Min Bid
#         Close: Time average ask
#         volume: N/A
#     """

#     values = []
#     for group_key, df in grouped:
#         if df.empty:
#             continue

#         if row.trading_class == "ZC":
#             print(df)
#             exit()

#         random_row = df.sample().iloc[0]
#         average_spread = random_row.close - random_row.open
#         values.append(average_spread)

#     average_spread: float = pd.Series(values).mean()
#     i = 7
#     if average_spread == 0.0:
#         print(f"{row.trading_class}: {average_spread:.{i}f} failed")
#     else:
#         print(f"{row.trading_class}: {average_spread:.{i}f}")
