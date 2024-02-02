
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
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

class MultiplePriceWrangler:
    
    def __init__(
        self,
        signal: RollSignal,
        continuous_data: list[ContinuousData],
        end_month: ContractMonth,
    ):
        
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
        
        self._end_month = end_month
        self._continuous_data = continuous_data
        self._signal = signal
        
    def process_bars(self, bars: list[Bar]):
        
        for bar in bars:
            
            # stop when the data module rolls to end month
            current_month = self._signal.chain.current_month
            if current_month >= self._end_month:
                break
            
            self.data_engine.process(bar)
            
            # self.cache.add_bar(bar)
            
            # for data in self._continuous_data:
            #     data.on_bar(bar)
        
        self._verify_result()
        
    def _verify_result(self) -> None:
        for data in self._continuous_data:
            
            if len(data.prices) == 0:
                raise ValueError(f"{data.instrument_id} daily len(self.prices) > 0")
            
            # trim prices to end month
            while data.prices[-1].current_month >= self._end_month:
                data.prices.pop(-1)
            
            last_month = data.prices[-1].current_month
            if last_month >= self._end_month:
                raise ValueError(f"last_month >= end_month for {data.instrument_id}")
            
            print(f"wrangling completed, last_month: {last_month}")
            
            
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
# instrument_provider = TestContractProvider(
#     approximate_expiry_offset=config.approximate_expiry_offset,
#     base=base,
# )
# self.chain = ContractChain(
#     config=config,
#     instrument_provider=instrument_provider,
# )

# # setup daily continuous prices
# self.daily_bar_type = daily_bar_type
# self.daily_prices = []
# self.daily_continuous = ContinuousData(
#     bar_type=daily_bar_type,
#     chain=self.chain,
#     start_month=start_month,
#     # end_month=end_month,
#     handler=self.daily_prices.append,
#     raise_expired=False,
#     ignore_expiry_date=True,
# )
# self.daily_continuous.register_base(
#     portfolio=portfolio,
#     msgbus=msgbus,
#     cache=self.cache,
#     clock=clock,
#     logger=logger,
# )
# self.daily_continuous.start()

# # setup minute continuous prices
# self.minute_prices = []
# self.minute_continuous = ContinuousData(
#     bar_type=minute_bar_type,
#     chain=self.chain,
        #     start_month=start_month,
        #     # end_month=end_month,
        #     handler=self.minute_prices.append,
        #     raise_expired=False,
        #     ignore_expiry_date=True,
        #     manual_roll=True,
        # )