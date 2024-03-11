import joblib
import pandas as pd

from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs


"""
once per hour should be enough
"""
def get_spread_value(row):
    bid_path = SPREAD_FOLDER / f"{row.uname}_BID.parquet"
    ask_path = SPREAD_FOLDER / f"{row.uname}_ASK.parquet"

    bid_df = pd.read_parquet(bid_path)
    ask_df = pd.read_parquet(ask_path)
    
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
    
    print(len(df))
    # TODO: why is this the same length after filtering by liquid hours
    mask = df.timestamp.apply(row.liquid_schedule.is_open)
    df = df[mask]
    assert not df.empty
    print(len(df))
    
    exit()

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
    error with schedule:
    
    JBLM: schedule error
    JBL: schedule error
    FTI: schedule error
    FCE: schedule error
    MFA: schedule error
    MFC: schedule error
    CN: schedule error
    TWN: schedule error
    SGP: schedule error
    6A: schedule error
    6J: schedule error
    IU: schedule error
    M6A: schedule error
    6N: schedule error
    KU: schedule error
    UC: schedule error
    FEF: schedule error
    """
    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    rows = [r for r in rows if r.uname == "FEF"]
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
