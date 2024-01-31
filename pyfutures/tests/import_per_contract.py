from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import Symbol
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from pytower.data.writer import BarParquetWriter
from ibapi.contract import Contract as IBContract
from pandas.core.dtypes.dtypes import DatetimeTZDtype
from pytower.data.portara import PortaraData
from pytower.data.files import ParquetFile
from nautilus_trader.model.data import Bar

from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import joblib


MONTH_LIST = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

from pyfutures.tests.adapters.interactive_brokers.test_kit import PER_CONTRACT_FOLDER

def process(
    path: Path,
    row: dict,
):

    contract_month = ContractMonth(path.stem[-5:])
    aggregation = path.parent.parent.stem
    instrument_id = InstrumentId(
        symbol=Symbol(row.base.id.symbol.value + "=" + contract_month.value),
        venue=row.base.id.venue,
    )

    bar_type = BarType.from_str(
        f"{instrument_id}-1-{aggregation}-MID-EXTERNAL"
    )
    
    outfile = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )

    # if outfile.path.exists():
    #     print(f"Skipping {path}...")
    #     return
    # else:
    #     print(f"Importing {path}...")
    
    df = PortaraData.read_dataframe(path)
    
    writer = BarParquetWriter(
        path=outfile.path,
        bar_type=bar_type,
        price_precision=row.base.price_precision,
        size_precision=1,
    )
    
    print(f"Writing {bar_type} {outfile}...")
    writer.write_dataframe(df)

def func_gen():
    
    rows = IBTestProviderStubs.universe_rows(
        filter=["ECO"],
    )
    
    for row in rows:
        
        paths = set(
            PortaraData.get_paths(row.data_symbol, BarAggregation.DAY) \
            + PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
        )
        
        for path in paths:
            yield joblib.delayed(process)(path, row)
            
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