
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

from nautilus_trader.core.datetime import unix_nanos_to_dt
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

class ContinuousPriceWrangler:
    
    def __init__(
        self,
        bar_type: BarType,
        start_month: ContractMonth,
        config: FuturesChainConfig,
        ):
        
        self.start_month = start_month
        self.bar_type = bar_type
        
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

        self.cache = Cache(
            logger=logger,
        )

        data_engine = DataEngine(
            msgbus=msgbus,
            cache=self.cache,
            clock=clock,
            logger=logger,
            config=DataEngineConfig(debug=True),
        )

        portfolio = Portfolio(
            msgbus,
            self.cache,
            clock,
            logger,
        )
        
        self.chain = FuturesChain(config=config)
        
        self.prices = []
        self.continuous = ContinuousData(
            bar_type=self.bar_type,
            chain=self.chain,
            start_month=start_month,
            # end_month=end_month,
            handler=self.prices.append,
        )
        
        self.continuous.register_base(
            portfolio=portfolio,
            msgbus=msgbus,
            cache=self.cache,
            clock=clock,
            logger=logger,
        )

        self.continuous.start()
        data_engine.start()
    
    def process_files(self, paths: list[Path]):
        
        # filter the paths by the hold cycle and start year
        filtered = []
        for path in paths:
            month = ContractMonth(path.stem.split("=")[1].split(".")[0])
            if month in self.chain.hold_cycle and month.year >= self.start_month.year:
                filtered.append(path)
        paths = filtered
        
        bars = []
        # merge and sort the bars and add find is_last boolean
        data = []
        for path in paths:
            file = ParquetFile.from_path(path)
            bars = file.read_objects()
            for i, bar in enumerate(bars):
                is_last = i == len(bars) - 1
                data.append((bar, is_last))
        data = list(sorted(data, key= lambda x: x[0].ts_init))
        assert len(data) > 0
        
        # process
        end_month = ContractMonth("2023F")
        for i, x in enumerate(data):
            bar, is_last = x
            
            # stop when the data module rolls to year 2024
            if len(self.prices) > 0 and self.prices[-1].current_month >= end_month:
                self.prices.pop(-1)
                break  # done sampling
            
            self.cache.add_bar(bar)
            self.continuous.on_bar(bar, is_last=is_last)
        
        assert len(self.prices) > 0
        
        last_month = self.prices[-1].current_month
        last_year = self.prices[-1].current_month.year
        
        print(f"wrangling completed: last_year: {last_year}, last_month: {last_month}")
        
        if last_month >= end_month:
            raise ValueError(f"last_month >= end_month for {self.bar_type}")
        
        if last_year != 2022:
            raise ValueError(f"last_year != 2022 for {self.bar_type}")
        
        # file = ParquetFile(
        #     parent=Path("/Users/g1/Desktop/continuous/data/genericdata_continuous_price"),
        #     bar_type=self.bar_type,
        #     cls=ContinuousPrice,
        # )
        # writer = ContinuousPriceParquetWriter(path=file.path)
        # writer.write_objects(data=self.prices)