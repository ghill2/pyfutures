import pandas as pd
import pytest
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.continuous.config import ContractChainConfig
from nautilus_trader.continuous.config import RollConfig
from nautilus_trader.continuous.contract_month import LETTER_MONTHS
from nautilus_trader.continuous.contract_month import ContractMonth
from nautilus_trader.continuous.cycle import RollCycle
from nautilus_trader.continuous.data import ContinuousData
from nautilus_trader.continuous.data import ContractExpired
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


class TestContinuousData:
    def setup(self):
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
                    exchange="SIM",
                )
            )

        chain_config = ContractChainConfig(
            instrument_id=bar_type.instrument_id,
            roll_config=roll_config,
            start_month=ContractMonth("2021H"),
        )

        self._continuous_data = ContinuousData(
            bar_type=bar_type,
            chain_config=chain_config,
        )

        self.engine.add_actor(self._continuous_data)

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
