from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
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
DATA_FOLDER = Path("/Users/g1/Desktop/portara data george")
OUT_FOLDER = Path("/Users/g1/Desktop/per_contract")

def read_dataframe(path: Path) -> pd.DataFrame:
    
    with open(path, 'r') as file:
        column_count = len(file.readline().split(","))
        
    if path.suffix == ".bd" and column_count == 8: # daily .bd file
        names=["symbol", "day", "open", "high", "low", "close", "tick_count", "volume"]
        dtype={
            "symbol": str,
            "day": int,
            "open": np.float64,
            "high": np.float64,
            "low": np.float64,
            "close": np.float64,
            "tick_count": np.int64,
            "volume": np.float64,
        }
        
    elif path.suffix in (".txt", ".b01") and column_count == 7:  # daily .txt file
        names = ["day", "open", "high", "low", "close", "tick_count", "volume"]
        dtype = {
            "day": int,
            "open": np.float64,
            "high": np.float64,
            "low": np.float64,
            "close": np.float64,
            "tick_count": np.int64,
            "volume": np.float64,
        }
    elif path.suffix in (".txt", ".b01") and column_count == 8:  # minute .txt or .b01 file
        names = ["day", "time", "open", "high", "low", "close", "tick_count", "volume"]
        dtype = {
            "day": int,
            "time": int,
            "open": np.float64,
            "high": np.float64,
            "low": np.float64,
            "close": np.float64,
            "tick_count": np.int64,
            "volume": np.float64,
        }
    else:
        print(path)
        raise RuntimeError(str(path))
            
    df = pd.read_csv(
        path,
        names=names,
        dtype=dtype,
    )
    
    if "symbol" in df.columns:
        df.drop(["symbol"], axis=1, inplace=True)
        
    if "time" in df.columns:
        df['timestamp'] = pd.to_datetime(
            (df["day"] * 10000 + df["time"]).astype(str),
            format="%Y%m%d%H%M",
            utc=True,
        )
        df.drop(["time"], axis=1, inplace=True)
    else:
        df['timestamp'] = pd.to_datetime(
            df["day"],
            format="%Y%m%d",
            utc=True,
        )

    df.drop(["day", "tick_count"], axis=1, inplace=True)
        
    return df

def process(
    path: Path,
    row: dict,
):

        # settlement_time = pd.Timestamp(settlement_time, tz=timezone).tz_convert("UTC")
        
        contract_month = ContractMonth(path.stem[-5:])
        aggregation = path.parent.parent.stem
        instrument_id = InstrumentId(
            symbol=Symbol(row["base"].id.symbol.value + "=" + contract_month.value),
            venue=row["base"].id.venue,
        )
        bar_type = BarType.from_str(
            f"{instrument_id}-1-{aggregation}-MID-EXTERNAL"
        )
        
        outfile = ParquetFile(
            parent=OUT_FOLDER,
            bar_type=bar_type,
            cls=Bar,
                year=contract_month.year,
        )
        
        if outfile.path.exists():
            print(f"Skipping {path}...")
            return
        else:
            print(f"Importing {path}...")
        
        
        df = read_dataframe(path)
        
        def _convert_timestamp(value: pd.Timestamp) -> pd.Timestamp:
            """Add the settlement time to the bar timestamps"""
            return value.replace(
                hour=row["settlement_time"].hours,
                minute=row["settlement_time"].minutes,
                tzinfo=row["timezone"],
            ).tz_convert("UTC") + pd.Timedelta(seconds=1)
            
        df.timestamp = df.timestamp.apply(_convert_timestamp)
        
        writer = BarParquetWriter(
            path=outfile.path,
            bar_type=bar_type,
            price_precision=row["price_precision"],
            size_precision=1,
        )
        
        writer.write_dataframe(df)

def func_gen():
    universe = IBTestProviderStubs.universe_dataframe(
        filter=["EBM"],
    )
    
    for row in universe.to_dict(orient="records"):
        
        daily_folder = (DATA_FOLDER / "DAY" / row["data_symbol"])
        minute_folder = (DATA_FOLDER / "MINUTE" / row["data_symbol"])
        assert daily_folder.exists()
        assert minute_folder.exists()
        
        daily_paths = list(daily_folder.glob("*.txt")) + list(daily_folder.glob("*.bd"))
        assert len(daily_paths) > 0
        
        minute_paths = list(minute_folder.glob("*.txt")) + list(minute_folder.glob("*.b01"))
        assert len(minute_paths) > 0
            
        for path in daily_paths + minute_paths:
            yield joblib.delayed(process)(path, row)
            
def export_missing_months():
    """
    those missing EBM arent accessible for portara
    so we will either need to skip them or create them from the min data
    2013X, 2014F, 2014X, 2015F are missing
    """
if __name__ == "__main__":
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen())
            
            
# @pytest.mark.asyncio()
# async def test_load_precisions(client):
    
#     await client.connect()
    

    
# make sure each file has a related data symbol marked in the universe csv
    
    # missing = []
    # for data_symbol in universe.data_symbol.dropna():
    #     print(DATA_FOLDER / data_symbol)
    #     if not (DATA_FOLDER / data_symbol).exists():
    #         missing.append(data_symbol)
    # if len(missing) > 0:
    #     for data_symbol in missing:
    #         print(f"Missing data for {data_symbol}")
    #     exit()
        
    # # check files
    # paths = list(DATA_FOLDER.rglob("*.txt")) + list(DATA_FOLDER.rglob("*.bd"))
    # no_month = []
    # for path in paths:

    #     # check file has a letter month check
    #     letter_month = path.stem[-1]
    #     if letter_month not in MONTH_LIST:
    #         no_month.append(path)
    # if len(no_month) > 0:
    #     for stem in no_month:
    #         print(f"{stem}")
    #     exit()
        
        # # check file has data symbol
        # rows = universe[universe['data_symbol'] == file.parent.stem]
        # if rows.empty:
        #     raise ValueError(f"{file}")