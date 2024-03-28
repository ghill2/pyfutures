import gc
import itertools
import time
from pathlib import Path

import joblib
import pandas as pd
from nautilus_trader.model.enums import BarAggregation

from pyfutures.tests.test_kit import IBTestProviderStubs


def merge_dataframes_pandas(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    total_count = len(dfs)
    i = 0
    total = pd.DataFrame(columns=["timestamp"])
    while len(dfs) > 0:
        df = dfs.pop(-1)
        total = pd.merge_ordered(
            total,
            df,
            left_on="timestamp",
            right_on="timestamp",
            how="outer",
        )
        total.sort_values(by="timestamp", inplace=True)
        print(f"Merged: {len(df)} items. {i + 1}/{total_count}")
        del df
        gc.collect()
        i += 1
    del dfs
    gc.collect()
    return total


def _process(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    df = merge_dataframes_pandas([df1, df2])
    df.drop("timestamp", axis=1, inplace=True)
    df = df.corr()  # don't use ffil
    del df1
    del df2
    gc.collect()
    return df


def process_correlation(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    keys = list(data.keys())
    total = pd.DataFrame(index=keys, columns=keys)

    combinations = itertools.combinations(keys, 2)
    results: list[pd.DataFrame] = joblib.Parallel(n_jobs=-1, backend="loky")(
        joblib.delayed(_process)(data[comb[0]], data[comb[1]]) for comb in combinations
    )

    for df in results:
        for x in df.index:
            for y in df.columns:
                total.at[x, y] = df.at[x, y]

    gc.collect()
    return total


def insert_headers(df: pd.DataFrame) -> pd.DataFrame:
    format_func = lambda x: "{:.{}f}".format(x, 3)
    df = df.applymap(format_func)
    df = df.astype(str)

    df = df.reset_index().rename({"index": "uname"}, axis=1)

    df.reset_index(drop=True, inplace=True)

    df.loc[-3] = pd.Series(map(sector_map.get, df.columns), index=df.columns)
    df.loc[-2] = pd.Series(map(region_map.get, df.columns), index=df.columns)
    df.loc[-1] = pd.Series(map(sub_sector_map.get, df.columns), index=df.columns)
    df = df.sort_index(ascending=True).reset_index(drop=True)

    df.insert(1, "sector", list(map(sector_map.get, df.uname.values)))
    df.insert(2, "region", list(map(region_map.get, df.uname.values)))
    df.insert(3, "sub_sector", list(map(sub_sector_map.get, df.uname.values)))
    df.fillna("", inplace=True)

    return df


if __name__ == "__main__":
    rows = IBTestProviderStubs.universe_rows()

    sector_map = {row.uname: row.sector for row in rows}
    sub_sector_map = {row.uname: row.sub_sector for row in rows}
    region_map = {row.uname: row.region for row in rows}

    prices_dfs = {}
    returns_dfs = {}
    for row in rows:
        file = IBTestProviderStubs.adjusted_file(
            trading_class=row.trading_class,
            symbol=row.symbol,
            aggregation=BarAggregation.DAY,
        )

        df = pd.read_parquet(file.path)
        assert df.adjusted.notna().any()
        assert df.timestamp.is_monotonic_increasing

        # prices
        prices_dfs[row.uname] = pd.DataFrame(
            {
                "timestamp": df.timestamp.dt.floor("D").values,
                row.uname: df.adjusted.values,
            }
        )

        returns_dfs[row.uname] = pd.DataFrame(
            {
                "timestamp": df.timestamp.iloc[1:].dt.floor("D").values,
                row.uname: df.adjusted.diff().iloc[1:].values,
            }
        )

    start_time = time.perf_counter()
    prices_corr = process_correlation(prices_dfs)
    del prices_dfs
    gc.collect()

    returns_corr = process_correlation(returns_dfs)
    del returns_dfs
    gc.collect()

    prices_corr = insert_headers(prices_corr)
    returns_corr = insert_headers(returns_corr)

    stop_time = time.perf_counter()
    elapsed = stop_time - start_time

    print(prices_corr)
    print(returns_corr)
    print(f"Elapsed = {elapsed}s")

    out_folder = Path("/Users/g1/Desktop/correlation")
    out_folder.mkdir(parents=True, exist_ok=True)

    prices_corr.to_csv(out_folder / "prices.csv", index=False)
    returns_corr.to_csv(out_folder / "returns.csv", index=False)

    # outpath = "/Users/g1/Desktop/universe_correlations.xlsx"
    # df_prices.index = df_prices.index.astype(str)
    # df_returns.index = df_returns.index.astype(str)
    # with pd.ExcelWriter(
    #     outpath,
    #     engine="openpyxl",
    #     mode="w",
    # ) as writer:
    #     df_prices.to_excel(writer, sheet_name="source_prices", index=True)
    #     df_returns.to_excel(writer, sheet_name="source_returns", index=True)
    #     df_corr_meta.to_excel(writer, sheet_name="correlation", index=False)
    # series_returns = adjusted_prices.diff()
    # m_series = pd.Series([row.sector, row.region, row.sub_sector], name=str(row.instrument_id))
    # all_ids = []
    # all_sector = []
    # all_region = []
    # all_sub_sector = []
    # all_series_prices = []
    # all_series_returns = []
    # all_m_series = []
    # returns
    # returns_dfs.append(
    #     pd.DataFrame(
    #         {
    #             "timestamp": df.timestamp.iloc[1:].dt.floor("D").values,
    #             row.uname: df.adjusted.diff().iloc[1:].values
    #         }
    #     )
    # )
    # key="timestamp",
    # # Replace null values in 'timestamp' with values from 'timestamp_right'
    # total = total.with_columns(
    #     pl.when(total['timestamp'].is_null(), total['timestamp_right'])
    #     .otherwise(total['timestamp'])
    #     .alias('timestamp')
    # )
    # merged = []
    # for left, right in zip(
    #     total['timestamp'],
    #     total['timestamp_right'],
    # ):
    #     merged.append(right if left is None else left)

    # total = total.with_columns("timestamp", merged)
    # total.timestamp = merged
    # Drop the 'timestamp_right' column if needed
    # mask = total["timestamp"].is_null()
    # print(total.filter(mask))
    # ["timestamp"] = total[mask]["timestamp_right"]

    # def chunkify(l, n):
    #     """Yield n number of striped chunks from l."""
    #     new_list = []
    #     for i in range(0, n):
    #         chunk = l[i::n]
    #         if chunk != []:
    #             new_list.append(chunk)
    #     return new_list

    # def merge_dataframes_pandas(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    #     total_count = len(dfs)
    #     i = 0
    #     total = pd.DataFrame(columns=["timestamp"])
    #     while len(dfs) > 0:
    #         df = dfs.pop(-1)
    #         # prices_corr = prices_corr.set_index("timestamp").combine_first(df.set_index("timestamp")).reset_index()
    #         total = pd.merge_ordered(
    #             total,
    #             df,
    #             left_on="timestamp",
    #             right_on="timestamp",
    #             how="outer",
    #         )
    #         total.sort_values(by="timestamp", inplace=True)
    #         print(f"Merged: {len(df)} items. {i + 1}/{total_count}")
    #         del df
    #         gc.collect()
    #         i += 1
    #     del dfs
    #     gc.collect()
    #     return total

    # def merge_dataframes_polars(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    #     total_count = len(dfs)
    #     i = 0
    #     total = dfs.pop(-1)
    #     while len(dfs) > 0:
    #         df = dfs.pop(-1)
    #         total = total.join(
    #             df,

    #             on="timestamp",
    #             how="outer",
    #         )

    #         total = total.with_columns([
    #             pl.when(total['timestamp'].is_null()) \
    #                 .then(total['timestamp_right']) \
    #                 .otherwise(total['timestamp']) \
    #                 .alias("timestamp")
    #         ])
    #         total = total.drop('timestamp_right')
    #         print(f"Merged: {len(df)} items. {i + 1}/{total_count}")
    #         del df
    #         gc.collect()
    #         i += 1
    #     del dfs
    #     gc.collect()
    #     return total

    # # df = merge_dataframes_polars(dfs)
    # for n_jobs in (24, 12, 6, 3):
    #     chunks = chunkify(dfs, n_jobs)
    #     assert len(chunks) == n_jobs
    #     dfs = joblib.Parallel(n_jobs=n_jobs, backend="loky")(
    #         joblib.delayed(merge_dataframes_polars)(chunk) for chunk in chunks
    #     )
    #     assert len(dfs) == n_jobs

    #     # n_jobs=12
    #     # chunks = chunkify(dfs, n_jobs)
    #     # assert len(chunks) == n_jobs
    #     # dfs = joblib.Parallel(n_jobs=n_jobs, backend="loky")(
    #     #     joblib.delayed(merge_dataframes_polars)(chunk) for chunk in chunks
    #     # )
    #     # assert len(dfs) == n_jobs

    #     # n_jobs=6
    #     # chunks = chunkify(dfs, n_jobs)
    #     # assert len(chunks) == n_jobs
    #     # dfs = joblib.Parallel(n_jobs=n_jobs, backend="loky")(
    #     #     joblib.delayed(merge_dataframes_polars)(chunk) for chunk in chunks
    #     # )
    #     # assert len(chunks) == n_jobs

    #     # n_jobs=3
    #     # chunks = chunkify(dfs, n_jobs)
    #     # assert len(chunks) == n_jobs
    #     # dfs = joblib.Parallel(n_jobs=n_jobs, backend="loky")(
    #     #     joblib.delayed(merge_dataframes_polars)(chunk) for chunk in chunks
    #     # )
    #     # assert len(chunks) == n_jobs

    # print("DONE")
    # df = merge_dataframes_polars(dfs)

    # exit()
