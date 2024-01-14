
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.wrangler import ContinuousPriceWrangler
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
import pandas as pd
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

"""
TODO: test if contract expires it take next forward price
TODO: test if forward contract expires before current contract expiry date
"""

def process_row(
    trading_class: str,
    symbol: str,
    hold_cycle: str,
    priced_cycle: str,
    roll_offset: str,
    approximate_expiry_offset: int,
    carry_offset: int,
    start: str,
    end: str,
):
    
    # load all the data for the data symbol
    data_folder = Path("/Users/g1/Desktop/output")
    
    instrument_id = InstrumentId.from_str(f"{trading_class}_{symbol}.IB")
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-MID-EXTERNAL")
    start_month = ContractMonth(start)
    
    wrangler = ContinuousPriceWrangler(
        bar_type=bar_type,
        start_month=start_month,
        config=FuturesChainConfig(
            instrument_id=instrument_id,
            hold_cycle=hold_cycle,
            priced_cycle=priced_cycle,
            roll_offset=roll_offset,
            approximate_expiry_offset=approximate_expiry_offset,
            carry_offset=carry_offset,
        )
    )
    
    keyword = f"{trading_class}_{symbol}=*.IB*.parquet"
    paths = list(sorted(data_folder.glob(keyword)))
    assert len(paths) > 0
    
    wrangler.process_files(paths)
    
    # start month is start of data
    # start month for debugging
    # idx = paths.index([x for x in paths if "2021Z" in x.stem][0])
    # paths = paths[idx:]
    # start_month = ContractMonth(paths[0].stem.split("=")[1].split(".")[0])
    
    #########################

def find_minimum_day_of_month_within_range(
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    dayname: str, # Friday, Tuesday etc
    dayofmonth: int, # 1, 2, 3, 4 etc
):
    
    # find minimum x day of each month within a date range
    import pandas as pd
    df = pd.DataFrame({'date': pd.date_range(start=start, end=end)})
    days = df[df['date'].dt.day_name() == dayname]

    data = {}
    for date in days.date:
        
        key = f"{date.year}{date.month}"
        if data.get(key) is None:
            data[key] = []
            
        data[key].append(date)
        
    filtered = []
    for key, values in data.items():
        filtered.append(values[dayofmonth - 1])

    return pd.Series(filtered).dt.day.min()

def find_problem_files():
    """
    find trading_classes where the files do have the hold cycle in every year
    """
    universe = IBTestProviderStubs.universe_dataframe()
    data_folder = Path("/Users/g1/Desktop/all UTC")
    for row in universe.itertuples():
        
        data_dir = (data_folder / row.data_symbol)
        paths = list(sorted(list(data_dir.glob("*.txt")) + list(data_dir.glob("*.b01"))))
        assert len(paths) > 0
        
        start_month = ContractMonth(row.data_start)
        end_month = ContractMonth(row.data_end)
        required_months = []
        
        cycle = RollCycle(row.hold_cycle)
        while True:
            required_months.append(start_month)
            start_month = cycle.next_month(start_month)
            if start_month >= end_month:
                break
        
        stems = [
            x.stem[-5:] for x in paths
        ]
        for month in required_months:
            if month.value in stems:
                continue
            print(row.trading_class, month.value)
        
if __name__ == "__main__":
    	
    # min_ = find_minimum_day_of_month_within_range(
    #     start=ContractMonth("1999H").timestamp_utc,
    #     end=ContractMonth("2025H").timestamp_utc,
    #     dayname="Thursday",
    #     dayofmonth=2,
    # )
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    for row in universe.itertuples():
        if row.trading_class == "MES":
            process_row(
                str(row.trading_class),
                str(row.symbol),
                str(row.hold_cycle),
                str(row.priced_cycle),
                int(row.roll_offset),
                int(row.expiry_offset),
                int(row.carry_offset),
                str(row.start),
                str(row.end),
            )
    
    # func_gen = (
    #     joblib.delayed(process_row)(
    #         str(row.trading_class),
    #         str(row.symbol),
    #         str(row.hold_cycle),
    #         str(row.priced_cycle),
    #         int(row.roll_offset),
    #         int(row.expiry_offset),
    #         int(row.carry_offset),
    #         str(row.start),
    #         str(row.end),
    #     )
    #     for row in universe.itertuples()
    #     if row.trading_class in [
    #         "FESB",
    #         "FESU",
    #         "FSTA",
    #         "FSTS",
    #         "FSTO",
    #         "FSTH",
    #     ]
    # )
    # results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen)


