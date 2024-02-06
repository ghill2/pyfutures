from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

import joblib
import gc
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.data.files import ParquetFile
import pandas as pd
from pyfutures.continuous.adjusted import AdjustedPrices
from pathlib import Path
from nautilus_trader.model.enums import BarAggregation
import polars as pl

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

if __name__ == "__main__":

    rows = IBTestProviderStubs.universe_rows()[:10]
    
    sector_map = {row.uname:row.sector for row in rows}
    sub_sector_map = {row.uname:row.sub_sector for row in rows}
    region_map = {row.uname:row.region for row in rows}
    
    dfs = {}
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
        dfs[row.uname] = pd.DataFrame(
                {
                    "timestamp": df.timestamp.dt.floor("D").values,
                    row.uname: df.adjusted.values
                }
            )
    
    
    for row1 in rows:
        for row2 in rows:
            if row1.uname == row2.uname:
                corr = 1
            else:
                df = merge_dataframes_pandas(
                    [dfs[row1.uname], dfs[row2.uname]]
                )
                print(df)
                exit()
                
    # merge the dataframes
    print("Merging dataframes")
    # prices_corr = pd.DataFrame(columns=["timestamp"])
    
    
    
    gc.collect()
    
    print("Performing correlation")
    df = pd.DataFrame(columns=["timestamp"])
    
    df.drop("timestamp", axis=1, inplace=True)
    corr = df.corr()  # don't use ffil
    corr["region"] = [region_map[x] for x in corr.index.values]
    corr["sector"] = [sector_map[x] for x in corr.index.values]
    corr["sub_sector"] = [sub_sector_map[x] for x in corr.index.values]
    print(corr)
        
    # returns_corr = pd.DataFrame(columns=["timestamp"])
    # for df in returns_dfs:
    #     returns_corr = returns_corr.merge(df, left_on="timestamp", right_on="timestamp", how="outer")
            
def correlate(df: pd.DataFrame) -> pd.DataFrame:
    pass
    
        
    

    
    
    
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