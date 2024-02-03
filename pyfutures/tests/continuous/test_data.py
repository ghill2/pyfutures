
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.providers import TestContractProvider
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
import pandas as pd
from nautilus_trader.model.data import Bar
from pyfutures.continuous.signal import RollSignal
from pyfutures.continuous.chain import ContractChain
from nautilus_trader.backtest.data_client import BacktestMarketDataClient
from nautilus_trader.model.identifiers import ClientId
from pyfutures.continuous.config import ContractChainConfig
from nautilus_trader.model.identifiers import InstrumentId
from pyfutures.continuous.cycle import RollCycle
            
class TestContinuousData:
    
    def __init__(self):
        clock = TestClock()
        logger = Logger(
            clock=TestClock(),
            level_stdout=LogLevel.DEBUG,
            # bypass=True,
        )

        msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=clock,
            logger=logger,
        )

        cache = Cache(logger=logger)
        
        portfolio = Portfolio(
            msgbus,
            cache,
            clock,
            logger,
        )
        
        client = BacktestMarketDataClient(
            client_id=ClientId("IB"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
        )
        
        self.data_engine = DataEngine(
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            config=DataEngineConfig(debug=True),
        )
        
        self.data_engine.register_client(client)
        
        self.chain_config = ContractChainConfig(
            instrument_id=InstrumentId.from_str("MES.IB"),
            hold_cycle=RollCycle("HMUZ"),
            priced_cycle=RollCycle("HMUZ"),
            roll_offset=-5,
            approximate_expiry_offset=14,
            carry_offset=1,
        )
        
    def test_one_signal_for_each_data(self):
        
        
        
        
        chain = ContractChain(
            config=self.chain_config,
            instrument_provider=TestContractProvider(),
        )
        
        data_d1 = ContinuousData(
            bar_type=BarType.from_str("MES.IB-1-DAY-MID-EXTERNAL"),
            signal=signal,
            chain=chain,
        )
        data_m1 = 
        
        
        for actor in continuous_data + [signal]:
            actor.register_base(
                portfolio=portfolio,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                logger=logger,
            )
            actor.start()
        
        self.data_engine.start()
            
        data = (
            "01-01-2021", 1, 2,
        )
    
    def test_one_signal_for_many_data():
        
        signal = RollSignal()
        
        pass