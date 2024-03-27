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

class TestContinuousDataReconcilicationDemo:
    
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
        