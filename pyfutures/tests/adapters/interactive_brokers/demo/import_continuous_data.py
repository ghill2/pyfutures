
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader.model.data import capsule_to_list
from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
import pandas as pd
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
        
from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import pytest

    
def get_start_month_and_year():
    
    data_folder = Path("/Users/g1/Downloads/portara data/all UTC")
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    for row in universe.itertuples():
        
        data_dir = (data_folder / row.data_symbol)
        
        assert data_dir.exists()
        
        files = list(sorted(list(data_dir.rglob("*.txt")) + list(data_dir.rglob("*.b01"))))
        start_month = files[0].stem[-1]
        start_year = files[0].stem[-5:-1]
        
        end_month = files[-1].stem[-1]
        end_year = files[-1].stem[-5:-1]
        print(end_year)

# def get_start_month_and_year():
    
#     data_folder = Path("/Users/g1/Downloads/portara data/all UTC")
    
#     universe = IBTestProviderStubs.universe_dataframe()
    
#     for row in universe.itertuples():
        
if __name__ == "__main__":
    
    
    # load all the data for the data symbol
    data_folder = Path("/Users/g1/Desktop/output")
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    for row in universe.itertuples():
        
        # these contracts failed to roll before expiry date, indication of lack of data to roll
        failed_list = ["CL"]
        if row.symbol in failed_list:
            continue
        
        keyword = f"{row.trading_class}-{row.symbol}=*.{row.exchange}*.parquet"
        paths = list(sorted(data_folder.glob(keyword)))
        
        # start = Path("/Users/g1/Desktop/output/HO-HO=2023V.NYMEX-1-MINUTE-MID-EXTERNAL-BAR-2023.parquet")
        # paths = paths[paths.index(start):]
        
        session = DataBackendSession()
        for i, path in enumerate(paths):
            session.add_file(NautilusDataType.Bar, f"data{i}", str(path))
            print(path)
        
        bars = []
        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            for bar in chunk:
                bars.append(bar)
        
        timestamps = [x.ts_event for x in bars]
        
        assert list(sorted(timestamps)) == timestamps
        
        instrument_id = f"{row.trading_class}-{row.symbol}.{row.exchange}"
        
        chain = FuturesChain(
            config=FuturesChainConfig(
                instrument_id=instrument_id,
                hold_cycle=row.hold_cycle,
                priced_cycle=row.priced_cycle,
                roll_offset=row.roll_offset,
                approximate_expiry_offset=row.expiry_offset,
                carry_offset=row.carry_offset,
            ),
        )
        
        # # user defined start month for debugging
        # start_year = int(start.stem.split("=")[1].split(".")[0][:4])
        # start_letter_month = start.stem.split("=")[1].split(".")[0][-1]
        # start_month = ContractMonth.from_year_letter_month(year=start_year, letter_month=start_letter_month)
        
        # start month is start of data
        start_year = int(paths[0].stem.split("=")[1].split(".")[0][:4])
        start_letter_month = paths[0].stem.split("=")[1].split(".")[0][-1]
        start_month = ContractMonth.from_year_letter_month(
                            year=int(row.start_year),
                            letter_month=row.start_month,
                        )
        
        
        prices = []
        data = ContinuousData(
            bar_type=bars[0].bar_type,
            chain=chain,
            start_month=start_month,
            # end_month=end_month,
            handler=prices.append,
        )
        
        #########################
        # component setup
        
        clock = TestClock()
        logger = Logger(
            clock=TestClock(),
            level_stdout=LogLevel.INFO,
            bypass=True,
        )

        msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=clock,
            logger=logger,
        )

        cache = Cache(
            logger=logger,
        )

        data_engine = DataEngine(
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            config=DataEngineConfig(debug=True),
        )

        portfolio = Portfolio(
            msgbus,
            cache,
            clock,
            logger,
        )
        
        data.register_base(
            portfolio=portfolio,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
        )

        data.start()
        data_engine.start()
        
        #########################
        
        data.on_start()
        
        end_month = ContractMonth("2024F")
        for bar in bars:
            
            # stop when the data module rolls to year 2024
            if len(prices) > 0 and prices[-1].current_month >= end_month:
                prices.pop(-1)
                assert prices[-1].current_month.year < 2024
                break  # done sampling
            cache.add_bar(bar)
            data.on_bar(bar)