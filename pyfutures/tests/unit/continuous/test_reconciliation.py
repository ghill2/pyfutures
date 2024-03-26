from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.continuous.chain import ContractChain


class TestContinuousDataReconcilication:
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

        self.chain = ContractChain(config=self.config)
        self.chain.register_base(
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )

    def test_reconcile_month_from_order(self):
        # the current month should be set to the month of the open position's contract
        pass

    def test_reconcile_data(self):
        # the continuous bars should be updated to current time when the strategy starts up
        # the continuous bars should cached
        pass
