from unittest.mock import Mock

import pandas as pd
import pytest
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
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

from pyfutures.continuous.bar import ContinuousBar
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.contract_month import LETTER_MONTHS
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.data import ContinuousData
from pyfutures.continuous.data import ContractExpired
from pyfutures.logger import LoggerAttributes


class TestContinuousData:
    def setup_method(self):
        LoggerAttributes.bypass = True
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

        for letter_month in LETTER_MONTHS:
            for year in (2020, 2021):
                self.engine.add_instrument(
                    TestInstrumentProvider.future(
                        symbol=f"MES={year}{letter_month}",
                        venue="SIM",
                    )
                )

        self.data = ContinuousData(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            config=RollConfig(
                hold_cycle=RollCycle("HMUZ"),
                priced_cycle=RollCycle("FGHJKMNQUVXZ"),
                roll_offset=-5,
                approximate_expiry_offset=14,
                carry_offset=1,
            ),
            start_month=ContractMonth("2021H"),
        )

        self.engine.add_actor(self.data)
        self.msgbus = self.engine.kernel.msgbus

    @pytest.mark.skip
    def test_subscriptions_after_start(self):
        sub_topics = [s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")]
        assert sub_topics == [
            "data.bars.MES=2021H.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
        ]

    def test_bar_types_return_expected_after_start(self):
        # Arrange & Act

        data = [
            ("MES=2021H.SIM", "2021-03-13"),
            ("MES=2021H.SIM", "2021-03-14"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        self.engine.run()

        # Assert
        assert self.data.current_bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.previous_bar_type == BarType.from_str("MES=2020Z.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.forward_bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert self.data.carry_bar_type == BarType.from_str("MES=2021J.SIM-1-DAY-MID-EXTERNAL")

    def test_current_bar_schedules_timer(self):
        # Arrange
        data = [
            ("MES=2021H.SIM", "2021-03-09"),
            ("MES=2021H.SIM", "2021-03-10"),
            ("MES=2021H.SIM", "2021-03-11"),
            ("MES=2021H.SIM", "2021-03-12"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        handle_mock = Mock()
        self.data._handle_time_event = handle_mock

        # Act
        self.engine.run()

        # Assert
        handle_mock.call_count == 4
        time_events = [call[0][0] for call in handle_mock.call_args_list]

        assert unix_nanos_to_dt(time_events[0].ts_init) == pd.Timestamp("2021-03-09 00:00:02+0000")
        assert unix_nanos_to_dt(time_events[1].ts_init) == pd.Timestamp("2021-03-10 00:00:02+0000")
        assert unix_nanos_to_dt(time_events[2].ts_init) == pd.Timestamp("2021-03-11 00:00:02+0000")

    def test_forward_bar_schedules_timer(self):
        # Arrange
        data = [
            ("MES=2021M.SIM", "2021-03-09"),
            ("MES=2021M.SIM", "2021-03-10"),
            ("MES=2021M.SIM", "2021-03-11"),
            ("MES=2021M.SIM", "2021-03-12"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        handle_mock = Mock()
        self.data._handle_time_event = handle_mock

        # Act
        self.engine.run()

        # Assert
        handle_mock.call_count == 4
        time_events = [call[0][0] for call in handle_mock.call_args_list]

        assert unix_nanos_to_dt(time_events[0].ts_init) == pd.Timestamp("2021-03-09 00:00:02+0000")
        assert unix_nanos_to_dt(time_events[1].ts_init) == pd.Timestamp("2021-03-10 00:00:02+0000")
        assert unix_nanos_to_dt(time_events[2].ts_init) == pd.Timestamp("2021-03-11 00:00:02+0000")

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

    def test_rolls_expected(self):
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
        assert self.data.current_month == ContractMonth("2021M")

    def test_bar_types_return_expected_after_roll(self):
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

    @pytest.mark.skip
    def test_subscriptions_after_roll(self):
        sub_topics = [s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")]
        assert sub_topics == [
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021U.SIM-1-DAY-MID-EXTERNAL",
        ]

    def test_not_create_continuous_bar_if_current_is_none(self):
        # Arrange
        data = [
            ("MES=2021M.SIM", "2021-03-09"),  # forward
            ("MES=2021M.SIM", "2021-03-10"),  # forward
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        handle_mock = Mock()
        self.data._handle_continuous_bar = handle_mock

        # Act
        self.engine.run()

        # Assert
        handle_mock.assert_not_called()

    def test_create_continuous_bar_if_current_is_not_none(self):
        # Arrange
        data = [
            ("MES=2021H.SIM", "2021-03-08"),  # current
            ("MES=2021M.SIM", "2021-03-08"),  # forward
            ("MES=2021H.SIM", "2021-03-09"),  # current
            ("MES=2021M.SIM", "2021-03-09"),  # forward
            ("MES=2021H.SIM", "2021-03-10"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        handle_mock = Mock()
        self.data._handle_continuous_bar = handle_mock

        # Act
        self.engine.run()

        # Assert
        assert handle_mock.call_count == 2

    def test_continuous_bar_has_expected_attributes(self):
        data = [
            ("MES=2020Z.SIM", "2021-03-09"),  # previous
            ("MES=2021H.SIM", "2021-03-09"),  # current
            ("MES=2021J.SIM", "2021-03-09"),  # carry
            ("MES=2021M.SIM", "2021-03-09"),  # forward
            ("MES=2021H.SIM", "2021-03-10"),  # previous
            ("MES=2021M.SIM", "2021-03-10"),  # current
            ("MES=2021N.SIM", "2021-03-10"),  # carry
            ("MES=2021U.SIM", "2021-03-10"),  # forward
            ("MES=2021H.SIM", "2021-03-11"),  # previous
            ("MES=2021M.SIM", "2021-03-11"),  # current
            ("MES=2021N.SIM", "2021-03-11"),  # carry
            ("MES=2021U.SIM", "2021-03-11"),  # forward
            ("MES=2021M.SIM", "2021-03-12"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)
        handle_mock = Mock()
        self.data._handle_continuous_bar = handle_mock

        # Act
        self.engine.run()

        # Assert
        assert handle_mock.call_count == 3

        bars: list[ContinuousBar] = [call[0][0] for call in handle_mock.call_args_list]

        # Assert
        assert bars[0].previous_bar.bar_type == BarType.from_str("MES=2020Z.SIM-1-DAY-MID-EXTERNAL")
        assert bars[0].current_bar.bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert bars[0].carry_bar.bar_type == BarType.from_str("MES=2021J.SIM-1-DAY-MID-EXTERNAL")
        assert bars[0].forward_bar.bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert bars[0].ts_init == dt_to_unix_nanos(pd.Timestamp("2021-03-09 00:00:02+0000"))
        assert bars[0].roll_ns == dt_to_unix_nanos(pd.Timestamp("2021-03-10 00:00:00+0000"))
        assert bars[0].expiration_ns == dt_to_unix_nanos(pd.Timestamp("2021-03-15 00:00:00+0000"))

        assert bars[1].previous_bar.bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert bars[1].current_bar.bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert bars[1].carry_bar.bar_type == BarType.from_str("MES=2021N.SIM-1-DAY-MID-EXTERNAL")
        assert bars[1].forward_bar.bar_type == BarType.from_str("MES=2021U.SIM-1-DAY-MID-EXTERNAL")
        assert bars[1].ts_init == dt_to_unix_nanos(pd.Timestamp("2021-03-10 00:00:02+0000"))
        assert bars[1].roll_ns == dt_to_unix_nanos(pd.Timestamp("2021-06-10 00:00:00+00:00"))
        assert bars[1].expiration_ns == dt_to_unix_nanos(pd.Timestamp("2021-06-15 00:00:00+00:00"))

        assert bars[2].previous_bar.bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert bars[2].current_bar.bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert bars[2].carry_bar.bar_type == BarType.from_str("MES=2021N.SIM-1-DAY-MID-EXTERNAL")
        assert bars[2].forward_bar.bar_type == BarType.from_str("MES=2021U.SIM-1-DAY-MID-EXTERNAL")
        assert bars[2].ts_init == dt_to_unix_nanos(pd.Timestamp("2021-03-11 00:00:02+0000"))
        assert bars[2].roll_ns == dt_to_unix_nanos(pd.Timestamp("2021-06-10 00:00:00+00:00"))
        assert bars[2].expiration_ns == dt_to_unix_nanos(pd.Timestamp("2021-06-15 00:00:00+00:00"))

    @pytest.mark.skip
    def test_continuous_bar_updates_deque(self):
        self.data._attempt_roll = Mock()

        data = [
            ("MES=2021H.SIM", 1.0, "MES=2021M.SIM", 10.1, "2021-03-04"),
        ]
        bars = self._create_continuous_bars(data)
        self.data._handle_continuous_bar(bars[0])

        assert len(self.data.continuous) == 1
        assert isinstance(self.data.continuous[0], ContinuousBar)

    @pytest.mark.skip
    def test_continuous_bar_updates_adjusted(self):
        self.data._attempt_roll = Mock()

        data = [
            ("MES=2021H.SIM", 1.0, "MES=2021M.SIM", 10.1, "2021-03-04"),
        ]
        bars = self._create_continuous_bars(data)
        self.data._handle_continuous_bar(bars[0])

        assert len(self.data.adjusted) == 1
        assert isinstance(self.data.adjusted[0], float)

    def _create_continuous_bars(self, data: list[tuple]) -> list[ContinuousBar]:
        # current_id, current_close, forward_id, forward_close, timestamp
        return [
            ContinuousBar(
                bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
                current_bar=Bar(
                    bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str("0.2"),
                    high=Price.from_str("100.0"),
                    low=Price.from_str("0.1"),
                    close=Price.from_str(str(row[1])),
                    volume=Quantity.from_int(1),
                    ts_event=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
                    ts_init=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
                ),
                forward_bar=Bar(
                    bar_type=BarType.from_str(f"{row[2]}-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str("0.2"),
                    high=Price.from_str("100.0"),
                    low=Price.from_str("0.1"),
                    close=Price.from_str(str(row[3])),
                    volume=Quantity.from_int(1),
                    ts_event=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
                    ts_init=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
                ),
                previous_bar=None,
                carry_bar=None,
                ts_event=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
                ts_init=dt_to_unix_nanos(pd.Timestamp(row[4], tz="UTC")),
            )
            for row in data
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

    def test_continuous_bar_to_adjusted(self):
        """
            current_month  current_price previous_month  previous_price  adj_value  adjusted
        0   MES=2021M.SIM             10  MES=2021H.SIM               1      196.0     206.0
        1   MES=2021M.SIM             11  MES=2021H.SIM               2      196.0     207.0
        2   MES=2021M.SIM             12  MES=2021H.SIM               3      196.0     208.0
        3   MES=2021M.SIM             13  MES=2021H.SIM               4      196.0     209.0
        4   MES=2021U.SIM            210  MES=2021M.SIM              14     2850.0    3060.0
        5   MES=2021U.SIM            220  MES=2021M.SIM              15     2850.0    3070.0
        6   MES=2021U.SIM            230  MES=2021M.SIM              16     2850.0    3080.0
        7   MES=2021U.SIM            240  MES=2021M.SIM              17     2850.0    3090.0
        8   MES=2021Z.SIM           3100  MES=2021U.SIM             250        0.0    3100.0
        9   MES=2021Z.SIM           3200  MES=2021U.SIM             260        0.0    3200.0
        10  MES=2021Z.SIM           3300  MES=2021U.SIM             270        0.0    3300.0
        11  MES=2021Z.SIM           3400  MES=2021U.SIM             290        0.0    3400.0
        """
        data = [
            ("MES=2021M.SIM", 10, "MES=2021H.SIM", 1),
            ("MES=2021M.SIM", 11, "MES=2021H.SIM", 2),
            ("MES=2021M.SIM", 12, "MES=2021H.SIM", 3),
            ("MES=2021M.SIM", 13, "MES=2021H.SIM", 4),
            ("MES=2021U.SIM", 210, "MES=2021M.SIM", 14),
            ("MES=2021U.SIM", 220, "MES=2021M.SIM", 15),
            ("MES=2021U.SIM", 230, "MES=2021M.SIM", 16),
            ("MES=2021U.SIM", 240, "MES=2021M.SIM", 17),
            ("MES=2021Z.SIM", 3100, "MES=2021U.SIM", 250),
            ("MES=2021Z.SIM", 3200, "MES=2021U.SIM", 260),
            ("MES=2021Z.SIM", 3300, "MES=2021U.SIM", 270),
            ("MES=2021Z.SIM", 3400, "MES=2021U.SIM", 290),
        ]
        df = pd.DataFrame(
            data,
            columns=["current_month", "current_price", "previous_month", "previous_price"],
        )

        adjusted = self.data._continuous_to_adjusted(df)
        assert adjusted == [206.0, 207.0, 208.0, 209.0, 3060.0, 3070.0, 3080.0, 3090.0, 3100.0, 3200.0, 3300.0, 3400.0]  # no_qa

    # def test_adjustment_appends_on_unique_current_bar(self):
    #     # Arrange
    #     data = [
    #         ("MES=2021H.SIM", "2021-03-04", 1),
    #         ("MES=2021H.SIM", "2021-03-05", 2),
    #         ("MES=2021M.SIM", "2021-03-06", 10.1),
    #         ("MES=2021M.SIM", "2021-03-07", 10.2),
    #         ("MES=2021H.SIM", "2021-03-08", 3),
    #         ("MES=2021M.SIM", "2021-03-09", 10.3),
    #     ]

    #     bars = [
    #         Bar(
    #             bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
    #             open=Price.from_str("0.5"),
    #             high=Price.from_str("100"),
    #             low=Price.from_str("0.1"),
    #             close=Price.from_str(str(row[2])),
    #             volume=Quantity.from_int(1),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
    #         )
    #         for row in data
    #     ]

    #     self.engine.add_data(bars)

    #     # Act
    #     self.engine.run()

    #     # Assert
    #     assert list(self.data.adjusted) == [1.0, 2.0, 3.0]

    # def test_adjustment_sets_expected(self):
    #     # Arrange
    #     data = [
    #         ("MES=2021H.SIM", "2021-03-09", 1),
    #         ("MES=2021M.SIM", "2021-03-09", 10.0),
    #         ("MES=2021H.SIM", "2021-03-10", 2),
    #         ("MES=2021M.SIM", "2021-03-10", 10.1),  # roll
    #         ("MES=2021M.SIM", "2021-03-11", 10.2),
    #         ("MES=2021Z.SIM", "2021-03-11", 20.2),
    #         ("MES=2021M.SIM", "2021-03-12", 10.3),
    #     ]

    #     bars = [
    #         Bar(
    #             bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
    #             open=Price.from_str("0.5"),
    #             high=Price.from_str("100"),
    #             low=Price.from_str("0.1"),
    #             close=Price.from_str(str(row[2])),
    #             volume=Quantity.from_int(1),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
    #         )
    #         for row in data
    #     ]
    #     self.engine.add_data(bars)

    #     # Act
    #     self.engine.run()

    #     # Assert
    #     assert list(self.data.adjusted) == [9.1, 10.1, 10.2]

    # def test_get_bars_returns_expected(self):
    #     # Arrange & Act

    #     data = [
    #         ("MES=2021H.SIM", "2021-03-13"),
    #         ("MES=2020Z.SIM", "2021-03-13"),
    #         ("MES=2021J.SIM", "2021-03-14"),
    #     ]

    #     bars = self._create_bars(data)
    #     self.engine.add_data(bars)
    #     self.engine.run()

    #     # Assert
    #     assert self.data.current_bar.bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
    #     assert self.data.previous_bar.bar_type == BarType.from_str("MES=2020Z.SIM-1-DAY-MID-EXTERNAL")
    # def test_adjustment(self):
    #     # Arrange
    #     data = [
    #         ("MES=2021H.SIM", 1.0, "MES=2021H.SIM", 10.1, "2021-03-04"),
    #         ("MES=2021H.SIM", "2021-03-05", 2),
    #         ("MES=2021M.SIM", "2021-03-06", 10.1),
    #         ("MES=2021M.SIM", "2021-03-07", 10.2),
    #         ("MES=2021H.SIM", "2021-03-08", 3),
    #         ("MES=2021M.SIM", "2021-03-09", 10.3),
    #     ]
