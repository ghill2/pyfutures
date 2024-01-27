from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.model.identifiers import InstrumentId
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from pytower.data.writer import BarParquetWriter
from ibapi.contract import Contract as IBContract
from pandas.core.dtypes.dtypes import DatetimeTZDtype

from pytower.data.files import ParquetFile

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib

MONTH_LIST = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

def process(
    trading_class: str,
    symbol: str,
    data_symbol: str,
    min_tick: float,
    price_magnifier: int,
):
    
    # get files in the data folder
    data_dir = (data_folder / data_symbol)
    
    files = list(sorted(list(data_dir.glob("*.txt")) + list(data_dir.glob("*.b01"))))
    
    for file in files:
        
        letter_month = file.stem[-1]
        
        year = int(file.stem[-5:-1])
        
        contract_month = ContractMonth.from_year_letter_month(year=year, letter_month=letter_month)
        
        instrument_id = InstrumentId.from_str(
            f"{trading_class}_{symbol}={contract_month}.IB"
        )
        
        bar_type = BarType.from_str(
            f"{instrument_id}-1-MINUTE-MID-EXTERNAL"
        )
        
        outfile = ParquetFile(
            parent=Path("/Users/g1/Desktop/output"),
            bar_type=bar_type,
            cls=Bar,
            year=year,
        )
        
        if outfile.path.exists():
            print(f"Skipping {file}...")
            continue
        else:
            print(f"Importing {file}...")
        
        df = pd.read_csv(
            file,
            names=["day", "time", "open", "high", "low", "close", "tick_count", "volume"],
            dtype={
                "day": int,
                "time": int,
                "open": np.float64,
                "high": np.float64,
                "low": np.float64,
                "close": np.float64,
                "tick_count": np.int64,
                "volume": np.float64,
            },
        )
        assert len(df.columns) == 8
        
        
        
        writer = BarParquetWriter(
            path=outfile.path,
            bar_type=bar_type,
            price_precision=IBTestProviderStubs.price_precision(
                min_tick=min_tick,
                price_magnifier=price_magnifier,
            ),
            size_precision=1,
        )
        
        writer.write_dataframe(df)

if __name__ == "__main__":
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    # make sure each file has a related data symbol marked in the universe csv
    data_folder = Path("/Users/g1/Desktop/all UTC")
    for data_symbol in universe.data_symbol.dropna():
        print(data_folder / data_symbol)
        assert (data_folder / data_symbol).exists()

    # check files
    files = list(data_folder.rglob("*.txt")) + list(data_folder.rglob("*.b01"))
    for file in files:

        # check file has a letter month check
        letter_month = file.stem[-1]
        assert letter_month in MONTH_LIST
        
        # # check file has data symbol
        # rows = universe[universe['data_symbol'] == file.parent.stem]
        # if rows.empty:
        #     raise ValueError(f"{file}")
        
    # import data
    rows = list(universe.itertuples())
    # rows = [
    #     x for x in rows
    #     if x.trading_class.startswith("EBM")
    # ]
    
    func_gen = (
        joblib.delayed(process)(
            str(row.trading_class),
            str(row.symbol),
            str(row.data_symbol),
            float(row.min_tick),
            int(row.price_magnifier),
        )
        for row in rows
    )
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen)
            
            
# @pytest.mark.asyncio()
# async def test_load_precisions(client):
    
#     await client.connect()
    

    