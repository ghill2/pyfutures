
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pytower.data.files import ParquetFile
from pyfutures.continuous.adjusted import AdjustedPrices

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib

def import_missing_ebm_months():
    
    # load minute EBM bars from years 2013X, 2014F, 2014X, 2015F
    row = IBTestProviderStubs.universe_dataframe(filter=["EBM"]).to_dict(orient="records")[0]
    months = ("2013X", "2014F", "2014X", "2015F")
    bars_list = []
    for month in months:
        
        bar_type = BarType.from_str(f"EBM_EBM={month}.IB-1-MINUTE-MID-EXTERNAL")
        path = CONTRACT_DATA_FOLDER / f"{bar_type}-BAR-{month[:4]}.parquet"
        assert path.exists()
        
        df = ParquetFile.from_path(path).read()
        
        df.index = unix_nanos_to_dt_vectorized(df["ts_event"])
        
        df.drop(["ts_event", "ts_init"], inplace=True, axis=1)
        freq = BarSpecification(1, BarAggregation.MINUTE, PriceType.MID).timedelta
        
        ohlc_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "first",
        }
        
        df = df.resample(freq, closed="left", label="left").apply(ohlc_dict).dropna()
        df.index = df.index.floor("D")
        df.reset_index(inplace=True)
        
        df["ts_event"] = dt_to_unix_nanos_vectorized(df["ts_event"])
        df["ts_init"] = df["ts_event"].copy()
        df = df[
            ["open", "high", "low", "close", "volume", "ts_event", "ts_init"]
        ]
        
        df = BarParquetWriter.from_rust(df)
        
        bar_type = BarType.from_str(f"EBM_EBM={month}.IB-1-DAY-MID-EXTERNAL")
        wrangler = BarDataWrangler(
            bar_type=bar_type,
            instrument=row["base"],
        )
        bars = wrangler.process(data=df)
        bars_list.extend(bars)
    return bars_list

def func_gen():
    
    universe = IBTestProviderStubs.universe_dataframe(
        filter=["ECO"],
    )
    for row in universe.to_dict(orient="records"):
        instrument_id = row['base'].id
        paths = MULTIPLE_PRICES_FOLDER.glob(f"{instrument_id}*.parquet")
        
        for path in paths:
            yield joblib.delayed(process)(
                path=path,
                row=row,
            )
        
if __name__ == "__main__":
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen())