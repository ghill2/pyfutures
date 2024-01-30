from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.core.datetime import dt_to_unix_nanos
import pandas as pd
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from pathlib import Path
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.clock import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.data.engine import DataEngine
from pytower.data.writer import MultiplePriceParquetWriter
from pytower.data.files import ParquetFile
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.data import BarType
from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import pytest
import joblib
from nautilus_trader.model.data import Bar
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import TestContractProvider

OUT_FOLDER = Path("/Users/g1/Desktop/calendars")

def process_row(
    row: dict,
):
    instrument_provider = TestContractProvider(
        approximate_expiry_offset=row["config"].approximate_expiry_offset,
        base=row["base"],
    )
    
    chain = ContractChain(
        config=row["config"],
        instrument_provider=instrument_provider,
    )
    
    df = pd.DataFrame(columns=["month", "approximate_expiry_date", "roll_date"])
    
    end_month = ContractMonth("2023F")
    contract = chain.current_contract(row["start"])
    while contract.info["month"] <= end_month:
        
        df.loc[len(df)] = (
            contract.info["month"].value,
            unix_nanos_to_dt(contract.expiration_ns).strftime("%Y-%m-%d"),
            chain.roll_date_utc(contract).strftime("%Y-%m-%d"),
        )
        
        contract = chain.forward_contract(contract)
    
    path = OUT_FOLDER / f"{row['trading_class']}_roll_calendar.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    
if __name__ == "__main__":
    universe = IBTestProviderStubs.universe_dataframe(
        filter=["ECO"],
    )
    
    func_gen = (
        joblib.delayed(process_row)(row)
        for row in universe.to_dict(orient='records')
    )
    results = joblib.Parallel(n_jobs=20, backend="loky")(func_gen)