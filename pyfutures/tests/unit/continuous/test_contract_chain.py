import pandas as pd
import pytest
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


class TestContractChain:
    def setup_method(self):
        self.config = ContractChainConfig(
            instrument_id=InstrumentId.from_str("MES.SIM"),
            roll_config=RollConfig(
                hold_cycle=RollCycle("HMUZ"),
                priced_cycle=RollCycle("FGHJKMNQUVXZ"),
                roll_offset=-5,
                approximate_expiry_offset=14,
                carry_offset=1,
            ),
            start_month=ContractMonth("2021H"),
        )

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

    @pytest.mark.skip
    def test_publish_roll_event(self):
        pass

    def test_initialize_sets_expected_attributes(self):
        # Arrange & Act
        self.chain.start()

        # Assert
        assert len(self.chain.rolls) == 1
        assert self.chain.rolls.to_month.iloc[0] == ContractMonth("2021H")

        assert self.chain.current_month == ContractMonth("2021H")
        assert self.chain.previous_month == ContractMonth("2020Z")
        assert self.chain.forward_month == ContractMonth("2021M")
        assert self.chain.carry_month == ContractMonth("2021J")

        assert self.chain.expiry_date == pd.Timestamp("2021-03-15", tz="UTC")
        assert self.chain.roll_date == pd.Timestamp("2021-03-10", tz="UTC")

    def test_roll_sets_expected_attributes(self):
        # Arrange & Act
        self.chain.start()
        self.chain.roll()

        # Assert
        assert len(self.chain.rolls) == 2
        assert self.chain.rolls.to_month.iloc[1] == ContractMonth("2021M")

        assert self.chain.current_month == ContractMonth("2021M")
        assert self.chain.previous_month == ContractMonth("2021H")
        assert self.chain.forward_month == ContractMonth("2021U")
        assert self.chain.carry_month == ContractMonth("2021N")

        assert self.chain.expiry_date == pd.Timestamp("2021-06-15", tz="UTC")
        assert self.chain.roll_date == pd.Timestamp("2021-06-10", tz="UTC")
