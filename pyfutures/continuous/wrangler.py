
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
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
from nautilus_trader.model.data import Bar
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from pyfutures.continuous.chain import TestContractProvider

class ContinuousPriceWrangler:
    
    def __init__(
        self,
        bar_type: BarType,
        base: FuturesContract,
        start_month: ContractMonth,
        config: ContractChainConfig,
        skip_months: list[ContractMonth] | None = None
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
        
        instrument_provider = TestContractProvider(
            approximate_expiry_offset=config.approximate_expiry_offset,
            base=base,
        )
        self.chain = ContractChain(
            config=config,
            instrument_provider=instrument_provider,
        )
        
        self.prices = []
        self.continuous = ContinuousData(
            bar_type=self.bar_type,
            chain=self.chain,
            start_month=start_month,
            # end_month=end_month,
            handler=self.prices.append,
            raise_expired=False,
            ignore_expiry_date=True,
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
        
        self._skip_months = skip_months or []
    
    def process_bars(self, bars: list[Bar]):
            
        # process
        end_month = ContractMonth("2023F")
        
        for bar in bars:
                        
            # stop when the data module rolls to year 2024
            if len(self.prices) > 0 and self.prices[-1].current_month >= end_month:
                self.prices.pop(-1)
                break  # done sampling
            print(bar)
            self.cache.add_bar(bar)
            
            self.continuous.on_bar(bar)
        
        if len(self.prices) == 0:
            raise ValueError(f"{self.bar_type} len(self.prices) > 0")
        
        last_month = self.prices[-1].current_month
        last_year = self.prices[-1].current_month.year
        
        print(f"wrangling completed: last_year: {last_year}, last_month: {last_month}")
        
        if last_month >= end_month:
            raise ValueError(f"last_month >= end_month for {self.bar_type}")
        
        if last_year != 2022:
            raise ValueError(f"last_year != 2022 for {self.bar_type}")
        
        return self.prices
        
        # if is_last:
            #     last_received = True
            # elif last_received:
            #     raise ValueError(f"contract failed to roll before or on last bar of current contract {self.continuous.current_id}")
            #     last_received = False
                
                # if self.current_id.month in self._ignore_failed:
                #     print("ignoring failed and rolling to next contract")
                #     self.current_id = self._chain.forward_id(self.current_id)
                #     self.roll(self._chain.forward_id(self.current_id))
                # else:
                    
        
            # elif self.continuous.current_id.month in self._skip_months:
            #     print(f"Skipping month {self.continuous.current_id.month}")
            #     self.continuous.roll()
            
            # last_received = False
            # def process_bars(self, bars_list: list[list[Bar]]):
        
    #     # TODO: check contract months
        
    #     # merge and sort the bars and add find is_last boolean
    #     data = []
    #     for bars in bars_list:
    #         for i, bar in enumerate(bars):
    #             is_last = i == len(bars) - 1
    #             data.append((bar, is_last))
    #     data = list(sorted(data, key= lambda x: x[0].ts_init))
    
    #     if len(data) == 0:
    #         raise ValueError(f"{self.bar_type} has no data")