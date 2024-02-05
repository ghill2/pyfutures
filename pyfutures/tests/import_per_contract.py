from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import Symbol
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from pyfutures.data.writer import BarParquetWriter
from pyfutures.data.portara import PortaraData
from pyfutures.data.files import ParquetFile
from nautilus_trader.model.data import Bar

from pathlib import Path
import pandas as pd
import numpy as np
import joblib

from pyfutures.tests.adapters.interactive_brokers.test_kit import PER_CONTRACT_FOLDER

def process(path: Path, row: dict) -> None:

    contract_month = ContractMonth(path.stem[-5:])
    aggregation = path.parent.parent.stem
    instrument_id = InstrumentId(
        symbol=Symbol(row.instrument_id.symbol.value + "=" + contract_month.value),
        venue=row.instrument_id.venue,
    )

    bar_type = BarType.from_str(
        f"{instrument_id}-1-{aggregation}-MID-EXTERNAL"
    )

    df = PortaraData.read_dataframe(path)
    
    writer = BarParquetWriter(
        path=file.path,
        bar_type=bar_type,
        price_precision=row.base.price_precision,
        size_precision=1,
    )
    
    file = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )
    print(f"Writing {bar_type} {file}...")
    
    writer.write_dataframe(df)
    
    # MINUTE -> HOUR
    if path.parent.parent.stem == "MINUTE":
        
        df = file.read(
            to_aggregation=(1, BarAggregation.HOUR),
        )
        
        bar_type = BarType.from_str(str(file.bar_type).replace("MINUTE", "HOUR"))
        
        file = ParquetFile(
            parent=PER_CONTRACT_FOLDER,
            bar_type=bar_type,
            cls=Bar,
        )
        
        writer = BarParquetWriter(
            path=file.path,
            bar_type=bar_type,
            price_precision=row.base.price_precision,
            size_precision=1,
        )
        
        print(f"Writing {bar_type} {file}...")
        writer.write_dataframe(df)

def func_gen():
    
    rows = IBTestProviderStubs.universe_rows(
        filter=["EBM"],
    )
    
    for row in rows:
        
        paths = list(sorted(set(
            PortaraData.get_paths(row.data_symbol, BarAggregation.DAY) \
            + PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
        )))
        
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