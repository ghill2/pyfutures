import pandas as pd

from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs


"""
once per hour should be enough
"""
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
    df = df[(df.bid != 0.0) & (df.ask != 0.0)]
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
        # is_open = row.liquid_schedule.is_open(
        #     pd.Timestamp("2024-02-28 18:34:00+00:00", tz="UTC"),
        # )
        df = df.iloc[-3000:]
        from pathlib import Path

        path = Path.home() / "Desktop/FTI.csv"
        df.to_csv(path, index=False)

    # TODO: why is this the same length after filtering by liquid hours
    # print(row.liquid_schedule.data)
    # start: 17:35:00+00:00
    # end: 20:59:00+00:00
    previous_len = len(df)
    mask = df.timestamp.apply(row.liquid_schedule.is_open)
    df = df[mask]
    assert not df.empty
    assert len(df) != previous_len

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


if __name__ == "__main__":
    """
    2024-02-28 21:00:00+00:00
    2024-02-28 23:45:00+00:00
    error with schedule:
    
    FTI:
    FCE:
    MFA:
    MFC:
    CN:
    TWN:
    SGP:
    IU:
    M6A:
    UC:
    FEF:
    """
    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    rows = [r for r in rows if r.uname == "FTI"]
    get_spread_value(rows[0])
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
