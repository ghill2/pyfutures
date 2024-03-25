import pandas as pd
import pytest
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.contract_month import LETTER_MONTHS
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.data import ContinuousData
from pyfutures.continuous.data import ContractExpired


class TestContinuousData:
    def setup_method(self):
        config = BacktestEngineConfig(
            logging=LoggingConfig(bypass_logging=True),
            run_analysis=False,
        )

        self.engine = BacktestEngine(config=config)

        self.engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
        )

        roll_config = RollConfig(
            hold_cycle=RollCycle("HMUZ"),
            priced_cycle=RollCycle("FGHJKMNQUVXZ"),
            roll_offset=-5,
            approximate_expiry_offset=14,
            carry_offset=1,
        )

        bar_type = BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL")

        for letter_month in LETTER_MONTHS:
            self.engine.add_instrument(
                TestInstrumentProvider.future(
                    symbol=f"MES=2021{letter_month}",
                    venue="SIM",
                )
            )

        chain_config = ContractChainConfig(
            instrument_id=bar_type.instrument_id,
            roll_config=roll_config,
            start_month=ContractMonth("2021H"),
        )

        self.data = ContinuousData(
            bar_type=bar_type,
            chain_config=chain_config,
        )

        self.engine.add_actor(self.data)
        self.msgbus = self.engine.kernel.msgbus

    def test_contract_expired_raises(self):
        # Arrange
        data = [
            ("MES=2021H.SIM", "2021-03-14"),
            ("MES=2021H.SIM", "2021-03-15"),  # expired
            ("MES=2021H.SIM", "2021-03-16"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)

        # Act & Assert
        with pytest.raises(ContractExpired):
            self.engine.run()

    def test_initialize_sets_expected_attributes(self):
        # Arrange & Act

        # Assert
        assert self.data.current_bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.previous_bar_type == BarType.from_str("MES=2020Z.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.forward_bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.carry_bar_type == BarType.from_str("MES=2021J.SIM-1-DAY-MID-EXTERNAL")

        sub_topics = [s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")]
        assert sub_topics == [
            "data.bars.MES=2021H.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
        ]

    def test_roll_sets_expected_attributes(self):
        # Arrange

        data = [
            ("MES=2021H.SIM", "2021-03-09"),
            ("MES=2021M.SIM", "2021-03-09"),
            ("MES=2021H.SIM", "2021-03-10"),
            ("MES=2021M.SIM", "2021-03-10"),
            ("MES=2021M.SIM", "2021-03-11"),  # rolled
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)

        # Act
        self.engine.run()

        # Assert
        assert self.data.current_bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.previous_bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.forward_bar_type == BarType.from_str("MES=2021U.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.carry_bar_type == BarType.from_str("MES=2021N.SIM-1-DAY-MID-EXTERNAL")

        sub_topics = [s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")]
        assert sub_topics == [
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021U.SIM-1-DAY-MID-EXTERNAL",
        ]

    def _create_bars(self, data: list[tuple]) -> list[Bar]:
        return [
            Bar(
                bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("1.1"),
                high=Price.from_str("1.2"),
                low=Price.from_str("1.0"),
                close=Price.from_str("1.1"),
                volume=Quantity.from_int(1),
                ts_event=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
                ts_init=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
            )
            for row in data
        ]
