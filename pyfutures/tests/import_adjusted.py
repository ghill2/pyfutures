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
    
    files = {
        BarAggregation.DAY: ParquetFile.from_path(paths[0]),
        BarAggregation.HOUR: ParquetFile.from_path(paths[1]),
        BarAggregation.MINUTE: ParquetFile.from_path(paths[2]),
    }
    
    multiple_prices = []
    for file in files.values():
        multiple_prices.extend(file.read_objects())
    multiple_prices = list(sorted(multiple_prices, key=lambda x: x.ts_init))
    
    adjusted = {
        BarAggregation.DAY: AdjustedPrices(
            bar_type=files[BarAggregation.DAY].bar_type,
            lookback=None,
            manual=False,
        ),
        BarAggregation.HOUR: AdjustedPrices(
            bar_type=files[BarAggregation.HOUR].bar_type,
            lookback=None,
            manual=True,
        ),
        BarAggregation.MINUTE: AdjustedPrices(
            bar_type=files[BarAggregation.MINUTE].bar_type,
            lookback=None,
            manual=True,
        ),
    }
    
    # multiple prices -> adjusted prices
    for price in multiple_prices:
        if price.bar_type.spec.aggregation == BarAggregation.DAY:
            value: float | None = adjusted[BarAggregation.DAY].handle_price(price)
            if value is not None:
                adjusted[BarAggregation.HOUR].adjust(value)
                adjusted[BarAggregation.MINUTE].adjust(value)
        elif price.bar_type.spec.aggregation == BarAggregation.HOUR:
            adjusted[BarAggregation.HOUR].handle_price(price)
        elif price.bar_type.spec.aggregation == BarAggregation.MINUTE:
            adjusted[BarAggregation.MINUTE].handle_price(price)
        else:
            raise RuntimeError()
            
    
    # write
    path = OUT_FOLDER / f"{row.trading_class}_adjusted.parquet"
    print(f"Writing {path}...")
    
    # write single series
    for aggregation in (BarAggregation.DAY, BarAggregation.HOUR, BarAggregation.MINUTE):
        path = ADJUSTED_PRICES_FOLDER / files[aggregation].path.name
        path.parent.mkdir(parents=True, exist_ok=True)
        df = adjusted[aggregation].to_dataframe()
        df = df[["timestamp", "adjusted"]]
        df.rename({"adjusted": "price"}, axis=0, inplace=True)
        df.to_parquet(path, index=False)
    
    # # write merged dataframe
    # df = pd.concat(
    #     [
    #         adjusted[BarAggregation.DAY].to_dataframe(),
    #         adjusted[BarAggregation.HOUR].to_dataframe(),
    #         adjusted[BarAggregation.MINUTE].to_dataframe(),
    #     ],
    # )
    # df.sort_values("timestamp", inplace=True)
    
    # path.parent.mkdir(parents=True, exist_ok=True)
    # df.to_parquet(path, index=False)
    
    # path = path.with_suffix(".csv")
    # print(f"Writing {path}...")
    # df.to_csv(path, index=False)

if __name__ == "__main__":
    
    rows = IBTestProviderStubs.universe_rows(
        # filter=["CL"],
    )
    
    results = joblib.Parallel(n_jobs=10, backend="loky")(
        joblib.delayed(process)(
            paths=list(MULTIPLE_PRICES_FOLDER.glob(f"{row.base.id}*.parquet")),
            row=row,
        )
        for row in rows
    )
    