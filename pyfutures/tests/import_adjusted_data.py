from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pytower.data.files import ParquetFile
from pyfutures.continuous.adjusted import AdjustedPrices

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib

MULTIPLE_PRICES_FOLDER = Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price")
OUT_FOLDER = Path("/Users/g1/Desktop/adjusted")

def process(
    path: Path,
    trading_class: str,
):
    
    print(trading_class, path)
    file = ParquetFile.from_path(path)
    continuous_prices = file.read_objects()
    
    # create adusted prices
    adjusted_prices = AdjustedPrices(lookback=None)
    
    # handle multiple prices on the adjusted prices
    for price in continuous_prices:
        adjusted_prices.handle_continuous_price(price=price)
    
    path = OUT_FOLDER / f"{trading_class}.parquet"
    # adjusted_prices.to_series().to_frame().to_parquet(path, index=False)
    print(path)
    
    # convert to series
    
    # save series as parquet
    
def func_gen():
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    paths = MULTIPLE_PRICES_FOLDER.glob("*.parquet")
    for path in paths:
        yield joblib.delayed(process)(
            path=path,
            trading_class=path.stem.split("_")[0],
        )
        
if __name__ == "__main__":
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen())
    # process(
    #     path=Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price/VX_VIX.IB-1-DAY-MID-EXTERNAL-CONTINUOUSPRICE-0.parquet"),
    #     trading_class="VX",
    # )