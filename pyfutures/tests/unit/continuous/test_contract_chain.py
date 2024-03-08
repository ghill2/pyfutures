from pathlib import Path
import itertools
import pandas as pd
from typing import Generator
from pyarrow.compute import Expression

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
import pytest
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.model.objects import Money
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.backtest.data_client import BacktestMarketDataClient
from nautilus_trader.model.objects import Money
from nautilus_trader.model.position import Position
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.events.order import OrderFilled
from nautilus_trader.backtest.models import FillModel
from decimal import Decimal
from nautilus_trader.accounting.factory import AccountFactory
from nautilus_trader.backtest.exchange import SimulatedExchange
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.backtest.data_client import BacktestDataClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import Money
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.backtest.execution_client import BacktestExecClient
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.nautilus_pyo3 import NautilusDataType
from nautilus_trader.risk.engine import RiskEngine

from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from pyfutures.continuous.providers import ContractProvider
from nautilus_trader.execution.config import ExecEngineConfig
from nautilus_trader.core.nautilus_pyo3 import DataBackendSession
from nautilus_trader.model.currencies import GBP
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.continuous.contract_month import ContractMonth

from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock
from nautilus_trader.model.identifiers import Venue
from pyfutures import PACKAGE_ROOT
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from pyfutures.continuous.chain import RollEvent
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth


from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

pytestmark = pytest.mark.skip

class TestContractChain:
    
    def setup_method(self):
        
        
        init_logging(level_stdout=LogLevel.DEBUG)

        self.clock = TestClock()
        self.msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=self.clock,
        )
        self.cache = TestComponentStubs.cache()
        self.data_engine = DataEngine(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=DataEngineConfig(debug=True),
        )
        self.portfolio = Portfolio(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.config = ContractChainConfig(
            instrument_id=InstrumentId.from_str("MES=MES=FUT.SIM"),
            hold_cycle=RollCycle("HMUZ"),
            priced_cycle=RollCycle("HMUZ"),
            roll_offset=-5,
            approximate_expiry_offset=14,
            carry_offset=1,
            start_month=ContractMonth("2021Z"),
        )
        
        self.bar_type = BarType.from_str("MES=MES=FUT.SIM-1-DAY-MID-EXTERNAL")
        
        instrument = FuturesContract.from_dict(
            {'type': 'FuturesContract', 'id': 'MES=MES=FUT.SIM', 'raw_symbol': 'MES', 'asset_class': 'COMMODITY', 'currency': 'USD', 'price_precision': 2, 'price_increment': '0.25', 'size_precision': 0, 'size_increment': '1', 'multiplier': '5.0', 'lot_size': '1', 'underlying': '', 'activation_ns': 0, 'expiration_ns': 0, 'margin_init': '0', 'margin_maint': '0', 'ts_event': 0, 'ts_init': 0}  # noqa
        )
        
        instrument_provider = ContractProvider(
            approximate_expiry_offset=self.config.approximate_expiry_offset,
            base=instrument,
        )
        
        self.chain = ContractChain(
            instrument_provider=instrument_provider,
            bar_type=self.bar_type,
            config=self.config,
        )
        
        self.chain.register(
            trader_id=TestIdStubs.trader_id(),
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        
        self.chain.start()
        self.data_engine.start()
    
    def test_swap_position_on_roll(self):
        
        instrument = FuturesContract.from_dict(
            {'type': 'FuturesContract', 'id': 'MES=MES=FUT=2021Z.SIM', 'raw_symbol': 'MES', 'asset_class': 'COMMODITY', 'currency': 'USD', 'price_precision': 2, 'price_increment': '0.25', 'size_precision': 0, 'size_increment': '1', 'multiplier': '5.0', 'lot_size': '1', 'underlying': '', 'activation_ns': 0, 'expiration_ns': 0, 'margin_init': '0', 'margin_maint': '0', 'ts_event': 0, 'ts_init': 0}  # noqa
        )
        
        position = Position(
            instrument=instrument,
            fill=OrderFilled(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=self.chain.id,
                instrument_id=instrument.id,
                client_order_id=ClientOrderId("1"),
                venue_order_id=VenueOrderId("2"),
                account_id=AccountId("SIM-001"),
                trade_id=TradeId("3"),
                position_id=PositionId("5"),
                order_side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                last_qty=Quantity.from_int(5),
                last_px=Price.from_int(1),
                currency=GBP,
                commission=Money(0, GBP),
                liquidity_side=LiquiditySide.TAKER,
                event_id=UUID4(),
                ts_event=dt_to_unix_nanos(pd.Timestamp("2021-12-09", tz="UTC")),
                ts_init=dt_to_unix_nanos(pd.Timestamp("2021-12-09", tz="UTC")),
            )
        )
        
        self.cache.add_position(
            position=position,
            oms_type=OmsType.NETTING,
        )
        
        open_positions = self.cache.positions_open(
            instrument_id=instrument.id,
        )
        assert len(open_positions) == 1
        
        commands = []
        self.msgbus.register(endpoint="RiskEngine.execute", handler=commands.append)
        
        data = [
            ("MES=MES=FUT=2022H.SIM", "2021-12-09", 10.0),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 10.1),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 2),  # roll
        ]
        
        bars = self._create_bars(data)
        for bar in bars:
            self.data_engine.process(bar)
        
        assert type(commands[0]) is SubmitOrder
        assert commands[0].instrument_id == InstrumentId.from_str('MES=MES=FUT=2021Z.SIM')
        assert commands[0].order.quantity == Quantity.from_int(5)
        assert commands[0].order.side == OrderSide.SELL
        
        assert type(commands[1]) is SubmitOrder
        assert commands[1].instrument_id == InstrumentId.from_str('MES=MES=FUT=2022H.SIM')
        assert commands[1].order.quantity == Quantity.from_int(5)
        assert commands[1].order.side == OrderSide.BUY
        
        
        
        
            
            
    def test_roll_sends_expected_roll_event(self):
        
        data = [
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 1),  # roll
            ("MES=MES=FUT=2022M.SIM", "2021-12-11", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-11", 1),
        ]
        
        events = []
        self.msgbus.subscribe(self.chain.topic, events.append)
        
        bars = self._create_bars(data)
        for i, bar in enumerate(bars):
            
            if i == 1:
                assert len(events) == 0
                
            self.data_engine.process(bar)
            
            if i == 1:
                assert len(events) == 1
                # assert events == [RollEvent(bar_type=BarType.from_str("MES=MES=FUT.SIM-1-DAY-MID-EXTERNAL"))]
            
    def test_roll_sets_expected_attributes(self):
        
        data = [
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 2),  # roll
        ]
        
        bars = self._create_bars(data)
        for bar in bars:
            self.data_engine.process(bar)
        
        assert self.chain.current_month == ContractMonth("2022H")
        assert self.chain.previous_month == ContractMonth("2021Z")
        assert self.chain.forward_month == ContractMonth("2022M")
        assert self.chain.carry_month == ContractMonth("2022M")
        
        assert self.chain.current_contract.id == InstrumentId.from_str("MES=MES=FUT=2022H.SIM")
        assert self.chain.previous_contract.id == InstrumentId.from_str("MES=MES=FUT=2021Z.SIM")
        assert self.chain.forward_contract.id == InstrumentId.from_str("MES=MES=FUT=2022M.SIM")
        assert self.chain.carry_contract.id == InstrumentId.from_str("MES=MES=FUT=2022M.SIM")
        
        assert self.chain.expiry_date == pd.Timestamp('2022-03-15', tz='UTC')
        assert self.chain.expiry_day == pd.Timestamp('2022-03-15', tz='UTC')
        assert self.chain.roll_date == pd.Timestamp('2022-03-10', tz='UTC')
        
        assert self.chain.current_bar_type == BarType.from_str("MES=MES=FUT=2022H.SIM-1-DAY-MID-EXTERNAL")
        assert self.chain.previous_bar_type == BarType.from_str("MES=MES=FUT=2021Z.SIM-1-DAY-MID-EXTERNAL")
        assert self.chain.forward_bar_type == BarType.from_str("MES=MES=FUT=2022M.SIM-1-DAY-MID-EXTERNAL")
        assert self.chain.carry_bar_type == BarType.from_str("MES=MES=FUT=2022M.SIM-1-DAY-MID-EXTERNAL")

        assert self.cache.instrument(self.chain.current_contract.id) is not None
        assert self.cache.instrument(self.chain.forward_contract.id) is not None
        assert self.cache.instrument(self.chain.carry_contract.id) is not None
        
        assert list(self.chain.rolls.timestamp) == [pd.Timestamp('2021-12-10', tz='UTC')]
        assert list(self.chain.rolls.to_month) == [ContractMonth("2022H")]
        
    def test_roll_to_start_month_on_start(self):
        assert self.chain.current_month == self.config.start_month
        
    def test_roll_handles_subscriptions(self):
        
        data = [
            ("MES=MES=FUT=2021Z.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 2),  # roll
        ]
        
        # before roll
        sub_topics = [
            s.topic for s in self.msgbus.subscriptions()
            if s.topic.startswith("data.bars.MES=MES=FUT")
        ]
        assert sub_topics == [
            'data.bars.MES=MES=FUT=2021Z.SIM-1-DAY-MID-EXTERNAL',
            'data.bars.MES=MES=FUT=2022H.SIM-1-DAY-MID-EXTERNAL',
        ]
        
        bars = self._create_bars(data)
        for bar in bars:
            self.data_engine.process(bar)
        
        # after roll
        sub_topics = [
            s.topic for s in self.msgbus.subscriptions()
            if s.topic.startswith("data.bars.MES=MES=FUT")
        ]
        assert sub_topics == [
            'data.bars.MES=MES=FUT=2022H.SIM-1-DAY-MID-EXTERNAL',
            'data.bars.MES=MES=FUT=2022M.SIM-1-DAY-MID-EXTERNAL',
        ]
    
    
        
        
    def test_current_bar_history(self):
        data = [
            ("MES=MES=FUT=2022H.SIM", "2021-12-09", 10.0),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 10.1),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 2),  # roll
            ("MES=MES=FUT=2022H.SIM", "2021-12-11", 10.2),
            ("MES=MES=FUT=2021M.SIM", "2021-12-11", 18.7),
        ]
        
        bars = self._create_bars(data)
        for bar in bars:
            self.data_engine.process(bar)
        
        instrument_ids = [
            bar.bar_type.instrument_id.value
            for bar in self.chain._current
        ]
        assert instrument_ids == [
            'MES=MES=FUT=2021Z.SIM',
            'MES=MES=FUT=2021Z.SIM',
            'MES=MES=FUT=2022H.SIM',
        ]
            
    
    def test_adjustment_current_forward_order(self):
        
        data = [
            ("MES=MES=FUT=2021Z.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-09", 10.0),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 2),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 10.1),  # roll
            ("MES=MES=FUT=2022H.SIM", "2021-12-11", 10.2),
            ("MES=MES=FUT=2022M.SIM", "2021-12-11", 18.7),
        ]
        
        bars = self._create_bars(data)
        captured = []
        for bar in bars:
            self.data_engine.process(bar)
            captured.append(
                list(self.chain._adjusted)
            )
        
        assert captured == [
            [1.0],
            [1.0],
            [1.0, 2.0],
            [9.1, 10.1],  # roll
            [9.1, 10.1, 10.2],
            [9.1, 10.1, 10.2],
        ]
                
    def test_adjustment_forward_current_order(self):
        
        data = [
            ("MES=MES=FUT=2022H.SIM", "2021-12-09", 10.0),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-09", 1),
            ("MES=MES=FUT=2022H.SIM", "2021-12-10", 10.1),
            ("MES=MES=FUT=2021Z.SIM", "2021-12-10", 2),  # roll
            ("MES=MES=FUT=2021M.SIM", "2021-12-11", 18.7),
            ("MES=MES=FUT=2022H.SIM", "2021-12-11", 10.2),
        ]
        
        bars = self._create_bars(data)
        captured = []
        for bar in bars:
            self.data_engine.process(bar)
            captured.append(
                list(self.chain._adjusted)
            )
        
        assert captured == [
            [],
            [1.0],
            [1.0],
            [9.1, 10.1],  # roll
            [9.1, 10.1],
            [9.1, 10.1, 10.2],
        ]
                
    def _create_bars(self, data: list[tuple]) -> list[Bar]:
        return [
            Bar(
                bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
                open=Price.from_str(str(row[2])),
                high=Price.from_str(str(row[2])),
                low=Price.from_str(str(row[2])),
                close=Price.from_str(str(row[2])),
                volume=Quantity.from_int(1),
                ts_event=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
                ts_init=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
            )
            for row in data
        ]
        
        
        

        #     if i == 633:  # before first roll
        #         print(bar.bar_type)
        #         list(self.chain.adjusted)[-3:] == [4699.0, 4667.0, 4711.0]

        #     if i == 634:  # after first roll on 2021-12-15
                
        #         list(self.chain.adjusted)[-3:] == [4706.5, 4674.5, 4718.5]
                
        #         # self.chain.adjusted[-1] == 4257.25

    # def _iterate_bars(self) -> Generator[Bar, None, None]:
        
    #     folder = Path(PACKAGE_ROOT) / "tests/data/test_continuous"
    #     session = DataBackendSession()
        
    #     filenames = [
    #         "MES=MES=FUT=2021Z.SIM-1-DAY-MID-EXTERNAL-0.parquet",
    #         "MES=MES=FUT=2022H.SIM-1-DAY-MID-EXTERNAL-0.parquet",
    #         "MES=MES=FUT=2022M.SIM-1-DAY-MID-EXTERNAL-0.parquet",
    #         # "MES=MES=FUT=2022U.SIM-1-DAY-MID-EXTERNAL-0.parquet",
    #         # "MES=MES=FUT=2022Z.SIM-1-DAY-MID-EXTERNAL-0.parquet",
    #     ]

    #     for i, filename in enumerate(filenames):
    #         path = folder / filename
    #         assert path.exists()
    #         session.add_file(NautilusDataType.Bar, f"table{i}", str(path))
        
        
    #     for chunk in session.to_query_result():
    #         chunk = capsule_to_list(chunk)
    #         yield from chunk
            
            

