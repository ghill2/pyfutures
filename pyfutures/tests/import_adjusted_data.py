from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pytower.data.files import ParquetFile
from pyfutures.continuous.adjusted import AdjustedPrices
from nautilus_trader.model.data import BarType

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib

MULTIPLE_PRICES_FOLDER = Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price")
OUT_FOLDER = Path("/Users/g1/Desktop/adjusted")

def process(
    paths: Path,
    row: dict,
):
    
    
    # create adusted prices
    instrument_id = row.base.id
    bar_type = BarType.from_str(f"{instrument_id}-1-DAY-EXTERNAL")
    adjusted_prices = AdjustedPrices(
        bar_type=bar_type,
        lookback=None,
    )
    
    # read prices
    continuous_prices = []
    for path in paths:
        file = ParquetFile.from_path(path)
        continuous_prices.extend(file.read_objects())
    
    # multiple prices -> adjusted prices
    for price in continuous_prices:
        adjusted_prices.handle_continuous_price(price=price)
    
    # write
    path = OUT_FOLDER / f"{path.stem}_adjusted.parquet"
    print(f"Writing {path}...")
    df = adjusted_prices.to_dataframe()
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    
    path = path.with_suffix(".csv")
    print(f"Writing {path}...")
    df.to_csv(path, index=False)
    
def func_gen():
    
    rows = IBTestProviderStubs.universe_rows(
        filter=["ECO"],
    )
    
    for row in rows:
        instrument_id = row.base.id
        paths = MULTIPLE_PRICES_FOLDER.glob(f"{instrument_id}*.parquet")
        yield joblib.delayed(process)(
            paths=paths,
            row=row,
        )
        
if __name__ == "__main__":
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen())
    