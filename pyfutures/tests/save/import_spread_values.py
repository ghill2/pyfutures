from pathlib import Path

import pandas as pd

from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs


def _merge_dataframe(bid_df: pd.DataFrame, ask_df: pd.DataFrame) -> pd.DataFrame:
    bid_df.sort_values(by="timestamp", inplace=True)
    ask_df.sort_values(by="timestamp", inplace=True)

    df = pd.merge(
        bid_df, ask_df, on="timestamp", how="inner", suffixes=("_bid", "_ask")
    )
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


def get_spread_value(row):
    print(row.uname)
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", None
    ):
        bid_df = pd.read_parquet(SPREAD_FOLDER / f"{row.uname}_BID.parquet")

        ask_df = pd.read_parquet(SPREAD_FOLDER / f"{row.uname}_ASK.parquet")

        df = _merge_dataframe(bid_df, ask_df)

        schedule = row.liquid_schedule

        start_time = df.timestamp.iloc[0].floor(pd.Timedelta(days=1))
        end_time = pd.Timestamp.utcnow().ceil(pd.Timedelta(days=1))

        open_times = schedule.to_open_range(
            start_date=start_time,
            end_date=end_time,
        )

        spread_values = []
        for i, open_time in enumerate(open_times):
            close_time = open_time + pd.Timedelta(hours=1)
            mask = (df.timestamp >= open_time) & (df.timestamp < close_time)
            ndf = df[mask]

            if ndf.empty:
                # print(f"{row.uname} {open_time}: No session")
                continue

            df["local"] = df.timestamp.dt.tz_convert(row.timezone)

            random_row = df.sample().iloc[0]
            average_spread = random_row.ask - random_row.bid
            spread_values.append(average_spread)
            # print(f"{row.uname} {open_time}: {average_spread:.2f}")

        assert len(spread_values) > 0

        average_spread: float = pd.Series(spread_values).mean()
        i = 7

        if average_spread == 0.0:
            print(f"{row.uname}: {average_spread:.{i}f} failed")
        else:
            print(f"{row.uname}: {average_spread:.{i}f}")

        return row.uname, average_spread


if __name__ == "__main__":
    rows = IBTestProviderStubs.universe_rows(
        # filter=["6A"],
        # skip=["6A"],
    )

    # for row in rows:
    #     get_spread_value(row)

    import joblib

    results = joblib.Parallel(n_jobs=-1, backend="loky")(
        joblib.delayed(get_spread_value)(row) for row in rows
    )
    df = pd.DataFrame(results, columns=["uname", "value"])
    print(df)
    path = Path.home() / "Desktop" / "spreads.csv"
    df.to_csv(path, index=False)

    # get_spread_value(rows[0])
    # for row in rows:
    #     asyncio.get_event_loop().run_until_complete(find_missing_sessions(row))

    # rows = [r for r in rows if r.uname == "FTI"]
    # get_spread_value(rows[0])
    # find_missing_sessions(rows[0])


# def find_missing_sessions(row):

#     end_time = pd.Timestamp.utcnow().floor(pd.Timedelta(days=1)) - pd.Timedelta(days=1)
#     start_time = end_time - pd.Timedelta(days=128)

#     path = SPREAD_FOLDER / f"{row.uname}_BID.parquet"
#     df = pd.read_parquet(path)

#     end_time = pd.Timestamp.utcnow().floor(pd.Timedelta(days=1)) - pd.Timedelta(days=1)
#     start_time = end_time - pd.Timedelta(days=128)
#     schedule = row.liquid_schedule
#     open_times = schedule.to_open_range(
#         start_date=start_time,
#         end_date=end_time,
#     )

#     for i, open_time in enumerate(open_times):
#         # open_time = open_times[3]
#         end = open_time + pd.Timedelta(hours=1)
#         start = open_time
#         # assert start.tzinfo == timezone.utc
#         # assert end.tzinfo == timezone.utc
#         # for t in df.timestamp:
#         #     assert t.tzinfo == pytz.UTC

#         timezone_to_country_code = {
#             timezone("US/Central"):"US",
#             timezone("US/Eastern"):"US",
#             timezone("GB-Eire"):"UK",
#             timezone("Japan"):"JP",
#         }

#         code = timezone_to_country_code[row.timezone]
#         hols = holidays.CountryHoliday(code, years=[2023, 2024])  # Adjust years as needed
#         with pd.option_context(
#             "display.max_rows",
#             None,
#             "display.max_columns",
#             None,
#             "display.width",
#             None,
#         ):
#             filt = df[(df.timestamp >= start) & (df.timestamp < end)]
#             # print(open_time, filt.empty)
#             if filt.empty:
#                 holiday = hols.get(start.date())

#                 if holiday is None:
#                     print(
#                         f"No data for session {i}/{len(open_times)}: {calendar.day_name[open_time.dayofweek]} {row.uname}. {start} > {end}"
#                     )
#                 else:
#                     print(
#                         f"Holiday {holiday} for session {i}/{len(open_times)}: {calendar.day_name[open_time.dayofweek]} {row.uname}. {start} > {end}"
#                     )


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
