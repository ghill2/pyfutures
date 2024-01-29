from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.wrangler import ContinuousPriceWrangler
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
from pyfutures.continuous.price import ContinuousPrice
from nautilus_trader.common.clock import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.data.engine import DataEngine
from pytower.data.writer import ContinuousPriceParquetWriter
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

def process_row(
    trading_class: str,
    symbol: str,
    hold_cycle: str,
    priced_cycle: str,
    roll_offset: str,
    approximate_expiry_offset: int,
    carry_offset: int,
    start: str,
    quote_currency: str,
    min_tick: float,
    price_magnifier: int,
    multiplier: float,
    missing_months: list[str] | None,
):
    instrument_id = InstrumentId.from_str(f"{trading_class}_{symbol}.IB")
    
    
    chain = ContractChain(
        config=ContractChainConfig(
            instrument_id=instrument_id,
            hold_cycle=hold_cycle,
            priced_cycle=priced_cycle,
            roll_offset=roll_offset,
            approximate_expiry_offset=approximate_expiry_offset,
            carry_offset=carry_offset,
            skip_months=missing_months,
        )
    )
    
    provider = TestContractProvider(
        approximate_expiry_offset=approximate_expiry_offset,
        base=FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=instrument_id.symbol,
            asset_class=AssetClass.ENERGY,
            currency=Currency.from_str(quote_currency),
            price_precision=price_precision,
            price_increment=price_increment,
            multiplier=Quantity.from_str(str(multiplier)),
            lot_size=Quantity.from_int(1),
            underlying="",
            activation_ns=0,
            expiration_ns=0,
            ts_event=0,
            ts_init=0,
        ),
    )
            
    
    
    end_month = ContractMonth("2023F")
    month = ContractMonth(start)
    df = pd.DataFrame()
    while month <= end_month:
        month = chain.next_month(month)
        
        

if __name__ == "__main__":
    universe = IBTestProviderStubs.universe_dataframe()
    print(universe)
    exit()
    func_gen = (
        joblib.delayed(process_row)(
            row.trading_class,
            row.symbol,
            row.hold_cycle,
            row.priced_cycle,
            row.roll_offset,
            row.expiry_offset,
            row.carry_offset,
            row.start,
            row.quote_currency.split("(")[1].split(")")[0],
            row.min_tick,
            row.price_magnifier,
            row.multiplier,
            row.missing_months.replace(" ", "").split(",") if type(row.missing_months) is not float else [],
        )
        for row in universe.itertuples()
    )
    results = joblib.Parallel(n_jobs=20, backend="loky")(func_gen)