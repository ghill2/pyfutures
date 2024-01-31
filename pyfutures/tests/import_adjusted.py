from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pytower.data.files import ParquetFile
from pyfutures.continuous.adjusted import AdjustedPrices
from nautilus_trader.model.data import BarType
from collections import namedtuple

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib
from nautilus_trader.model.enums import BarAggregation

from pyfutures.tests.adapters.interactive_brokers.test_kit import MULTIPLE_PRICES_FOLDER
from pyfutures.tests.adapters.interactive_brokers.test_kit import ADJUSTED_PRICES_FOLDER

OUT_FOLDER = ADJUSTED_PRICES_FOLDER

def process(
    paths: list[Path],
    row: namedtuple,
) -> None:
    
    
    # create adusted prices
    instrument_id = row.base.id
    # bar_type = BarType.from_str(f"{instrument_id}-1-DAY-MID-EXTERNAL")
    paths = list(sorted(paths))
    
    multiple_prices = []
    
    # read daily prices
    file = ParquetFile.from_path(paths[0])
    adjusted_prices_daily = AdjustedPrices(
        bar_type=file.bar_type,
        lookback=None,
        manual=False,
    )
    multiple_prices.extend(file.read_objects())
    
    # read minute prices
    file = ParquetFile.from_path(paths[1])
    adjusted_prices_minute = AdjustedPrices(
        bar_type=file.bar_type,
        lookback=None,
        manual=True,
    )
    multiple_prices.extend(file.read_objects())
    
    multiple_prices = list(sorted(multiple_prices, key=lambda x: x.ts_init))
    
    # multiple prices -> adjusted prices
    for price in multiple_prices:
        if price.bar_type.spec.aggregation == BarAggregation.DAY:
            adjustment_value: float | None = adjusted_prices_daily.handle_price(price)
            if adjustment_value is not None:
                adjusted_prices_minute.adjust(adjustment_value)
        elif price.bar_type.spec.aggregation == BarAggregation.MINUTE:
            adjusted_prices_minute.handle_price(price)
        else:
            raise RuntimeError()
            
    
    # write
    path = OUT_FOLDER / f"{row.trading_class}_adjusted.parquet"
    print(f"Writing {path}...")
    
    df = pd.concat(
        [
            adjusted_prices_minute.to_dataframe(),
            adjusted_prices_daily.to_dataframe(),
        ],
    )
    df.sort_values("timestamp", inplace=True)
    
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    
    path = path.with_suffix(".csv")
    print(f"Writing {path}...")
    df.to_csv(path, index=False)

if __name__ == "__main__":
    
    rows = IBTestProviderStubs.universe_rows(
        filter=["ECO"],
    )
    
    results = joblib.Parallel(n_jobs=-1, backend="loky")(
        joblib.delayed(process)(
            paths=list(MULTIPLE_PRICES_FOLDER.glob(f"{row.base.id}*.parquet")),
            row=row,
        )
        for row in rows
    )
    