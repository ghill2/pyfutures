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
    row: dict,
):
    
    file = ParquetFile.from_path(path)
    continuous_prices = file.read_objects()
    
    # create adusted prices
    adjusted_prices = AdjustedPrices(lookback=None)
    
    # handle multiple prices on the adjusted prices
    for price in continuous_prices:
        adjusted_prices.handle_continuous_price(price=price)
    
    path = OUT_FOLDER / f"{row['trading_class']}_adjusted.parquet"
    df = adjusted_prices.to_series().apply(float).to_frame()
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    
    path = path.with_suffix(".csv")
    df.to_csv(path)
    
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
    