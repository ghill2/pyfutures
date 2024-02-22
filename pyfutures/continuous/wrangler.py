from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import MultipleData
from nautilus_trader.portfolio.portfolio import Portfolio
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.component import Logger
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.backtest.data_client import BacktestMarketDataClient
from nautilus_trader.model.identifiers import ClientId
from pyfutures.continuous.multiple_bar import MultipleBar


class MultiplePriceWrangler:
    def __init__(
        self,
        continuous_data: list[MultipleData],
        end_month: ContractMonth,
        debug: bool = False,
    ):
        clock = TestClock()

        msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=clock,
        )

        cache = Cache()

        portfolio = Portfolio(
            msgbus,
            cache,
            clock,
        )

        client = BacktestMarketDataClient(
            client_id=ClientId("IB"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self.data_engine = DataEngine(
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=DataEngineConfig(debug=True),
        )

        self.data_engine.register_client(client)

        chains = list(set([x.chain for x in continuous_data]))
        for actor in chains + continuous_data:
            actor.register_base(
                portfolio=portfolio,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
            )
            actor.start()

        self.data_engine.start()

        self._end_month = end_month
        self._continuous_data = continuous_data

        self.prices = {}
        for data in continuous_data:
            self.prices[data.bar_type] = []
            msgbus.subscribe(
                topic=data.topic,
                handler=self.prices[data.bar_type].append,
            )

    def process_bars(self, bars: list[Bar]) -> dict[BarType, list[MultipleBar]]:
        for bar in bars:
            # month = ContractMonth(bar.bar_type.instrument_id.symbol.value.split("=")[-1])
            # if month >= self._end_month:
            #     continue

            self.data_engine.process(bar)

        self._verify_result()

        return self.prices

    def _verify_result(self) -> None:
        for data in self._continuous_data:
            if len(self.prices[data.bar_type]) == 0:
                raise ValueError(f"{data.instrument_id} daily len(self.prices) > 0")

            # trim prices to end month
            while self.prices[data.bar_type][-1].current_month >= self._end_month:
                self.prices[data.bar_type].pop(-1)

            last_month = self.prices[data.bar_type][-1].current_month
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
# # stop when the data module rolls to end month
# current_month = self._signal.chain.current_month
# if current_month >= self._end_month:
#     break
# self.cache.add_bar(bar)

# for data in self._continuous_data:
#     data.on_bar(bar)
