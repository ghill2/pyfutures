import pandas as pd
import pytest
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.enums import OmsType
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.model.position import Position
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.bar import ContinuousBar
from pyfutures.continuous.data import ContinuousData

class TestContinuousDataReconcilication:
    """
    reconciliation on external orders - changed position on ib manually, strategy should know about it
    test caching on startup and shutdown
    """
    def setup_method(self):
        self.clock = TestClock()
        self.msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=self.clock,
        )

        self.cache = TestComponentStubs.cache()

        self.portfolio = Portfolio(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        
        self.bar_type = BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL")
        self.data = ContinuousData(
            bar_type=self.bar_type,
            strategy_id=TestIdStubs.strategy_id(),
            config=RollConfig(
                hold_cycle=RollCycle("HMUZ"),
                priced_cycle=RollCycle("FGHJKMNQUVXZ"),
                roll_offset=-5,
                approximate_expiry_offset=14,
                carry_offset=1,
            ),
            reconciliation=True,
        )
        self.data.register_base(
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
    
    def test_reconcile_month_from_position(self):
        # the current month should be position's the month if it matches the last cached continuous bar
        
        instrument = TestInstrumentProvider.future(symbol="MES=2021H", venue="SIM")
        
        # add position to cache
        order = TestExecStubs.market_order(instrument=instrument)
        fill = TestEventStubs.order_filled(
            order=order,
            instrument=instrument,
            position_id=TestIdStubs.position_id(),
        )
        position = Position(instrument=instrument, fill=fill)
        self.cache.add_position(position, oms_type=OmsType.NETTING)
        
        # add instrument to cache
        self.cache.add_instrument(instrument)
        
        # add continuous Bar
        self.data.continuous.append(
            ContinuousBar(
                bar_type=self.bar_type,
                current_bar=Bar(
                    bar_type=BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str("1.00002"),
                    high=Price.from_str("1.00004"),
                    low=Price.from_str("1.00001"),
                    close=Price.from_str("1.00003"),
                    volume=Quantity.from_int(1_000_000),
                    ts_init=0,
                    ts_event=0,
                ),
                ts_event=0,
                ts_init=0,
                expiration_ns=0,
                roll_ns=0,
            )
            
        )
        
        # Act
        start_month = self.data.reconcile_month()
        
        # Assert
        assert start_month == ContractMonth("2021H")


    def test_reconcile_month_from_calendar(self):
        # if no position is found the start_month should be based on the roll calendar
        
        # Arrange
        now = pd.Timestamp("2024-06-10", tz="UTC")
        self.clock.advance_time(dt_to_unix_nanos(now))
        
        # Act
        start_month = self.data.reconcile_month()
        
        assert start_month == ContractMonth("2024M")
    
    @pytest.mark.skip
    def test_reconcile_data(self):
        # the continuous bars should be updated to current time when the strategy starts up
        # a request should be made for previous, carry, forward, current bar types from
        # timestamp of last continuous bar to now time
        
        self.data.continuous.append(
            ContinuousBar(
                bar_type=self.bar_type,
                current_bar=Bar(
                    bar_type=BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str("1.00002"),
                    high=Price.from_str("1.00004"),
                    low=Price.from_str("1.00001"),
                    close=Price.from_str("1.00003"),
                    volume=Quantity.from_int(1_000_000),
                    ts_init=0,
                    ts_event=0,
                ),
                ts_event=0,
                ts_init=0,
                expiration_ns=0,
                roll_ns=0,
            )
        )
        
        
        
