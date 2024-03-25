from pathlib import Path

import pandas as pd
import pytest
from nautilus_trader import PACKAGE_ROOT
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.nautilus_pyo3 import DataBackendSession
from nautilus_trader.core.nautilus_pyo3 import NautilusDataType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.data import TestDataStubs

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.wranglers import ContinuousBarWrangler


class TestContinuousWrangler:
    def setup(self):
        self.roll_config = RollConfig(
            hold_cycle=RollCycle("HNUZ"),
            priced_cycle=RollCycle("FHJMNUVZ"),
            roll_offset=-30,
            approximate_expiry_offset=27,
            carry_offset=1,
        )

        self.chain_config = ContractChainConfig(
            bar_type=BarType.from_str("HG.SIM-1-DAY-MID-EXTERNAL"),
            roll_config=self.roll_config,
            start_month=ContractMonth("2021H"),
        )

    def test_wrangler_outputs_expected(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )

        bars = self._read_bars()

        continuous_bars = wrangler.process(bars)
        assert len(continuous_bars) == 1322

        current_months = {ContractMonth(b.bar_type.instrument_id.symbol.value.split("=")[-1]) for b in continuous_bars}

        assert all(m in self.roll_config.hold_cycle for m in current_months)

    def test_wrangler_stops_at_end_month(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )

        bars = self._read_bars()
        continuous_bars = wrangler.process(bars)

        assert continuous_bars[-1].current_month == ContractMonth("2021U")
        assert continuous_bars[-2].current_month == ContractMonth("2021N")

    def test_validate_no_data_for_month_raises(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )
        data = [
            ("HG=1970H.SIM", "1970-01-01"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)
            assert "['2021H', '2021N', '2021Z']" in ex_info.value.args[0]

    def test_validate_no_timestamps_in_roll_window_raises(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )

        # no current in 2021H > 2021N roll window 2021-02-26 to 2021-03-28
        data = [
            ("HG=2021H.SIM", "1970-01-01"),
            ("HG=2021N.SIM", "1970-01-01"),
            ("HG=2021U.SIM", "1970-01-01"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)

        assert "2021H" in ex_info.value.args[0]
        assert "no timestamps" in ex_info.value.args[0]
        assert "2021-02-26 00:00:00+00:00 to 2021-03-28 00:00:00+00:00" in ex_info.value.args[0]

        # no forward in 2021H > 2021N roll window 2021-02-26 to 2021-03-28
        data = [
            ("HG=2021H.SIM", "2021-02-26"),
            ("HG=2021N.SIM", "1970-01-01"),
            ("HG=2021U.SIM", "1970-01-01"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)

        assert "2021N" in ex_info.value.args[0]
        assert "no timestamps" in ex_info.value.args[0]
        assert "2021-02-26 00:00:00+00:00 to 2021-03-28 00:00:00+00:00" in ex_info.value.args[0]

        # no current in 2021N > 2021U window 2021-06-28 to 2021-07-28
        data = [
            ("HG=2021H.SIM", "2021-02-26"),
            ("HG=2021N.SIM", "2021-02-26"),
            ("HG=2021U.SIM", "1970-01-01"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)

        assert "2021N" in ex_info.value.args[0]
        assert "no timestamps" in ex_info.value.args[0]
        assert "2021-06-28 00:00:00+00:00 to 2021-07-28 00:00:00+00:00" in ex_info.value.args[0]

        # no forward in 2021N > 2021U roll window 2021-06-28 to 2021-07-28
        data = [
            ("HG=2021H.SIM", "2021-02-26"),
            ("HG=2021N.SIM", "2021-02-26"),
            ("HG=2021N.SIM", "2021-06-28"),
            ("HG=2021U.SIM", "1970-01-01"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)

        assert "2021U" in ex_info.value.args[0]
        assert "no timestamps" in ex_info.value.args[0]
        assert "2021-06-28 00:00:00+00:00 to 2021-07-28 00:00:00+00:00" in ex_info.value.args[0]

    def test_validate_no_matching_timestamps_in_roll_window_raises(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )

        # no matching in 2021H > 2021N roll window 2021-02-26 to 2021-03-28
        data = [
            ("HG=2021H.SIM", "2021-02-26"),
            ("HG=2021N.SIM", "2021-02-27"),
            ("HG=2021N.SIM", "2021-06-28"),
            ("HG=2021U.SIM", "2021-06-28"),
        ]

        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)
            assert "2021H and 2021N" in ex_info.value.args[0]

        # no matching in 2021N > 2021U roll window 2021-06-28 to 2021-07-28
        data = [
            ("HG=2021H.SIM", "2021-03-26"),
            ("HG=2021N.SIM", "2021-03-26"),
            ("HG=2021N.SIM", "2021-06-28"),
            ("HG=2021U.SIM", "2021-08-29"),
        ]
        bars = self._create_bars(data)
        with pytest.raises(ValueError) as ex_info:
            wrangler.validate(bars)
            assert "2021N and 2021U" in ex_info.value.args[0]

    def test_validate_input_assertions(self):
        wrangler = ContinuousBarWrangler(
            config=self.chain_config,
            end_month=ContractMonth("2021U"),
        )

        # assert only one bar spec
        bars = [
            Bar(
                bar_type=BarType.from_str("HG=2021H.SIM-1-DAY-BID-EXTERNAL"),
                open=Price.from_str("90.002"),
                high=Price.from_str("90.004"),
                low=Price.from_str("90.001"),
                close=Price.from_str("90.003"),
                volume=Quantity.from_int(1_000_000),
                ts_event=0,
                ts_init=0,
            ),
            Bar(
                bar_type=BarType.from_str("HG=2021H.SIM-1-MINUTE-BID-EXTERNAL"),
                open=Price.from_str("90.002"),
                high=Price.from_str("90.004"),
                low=Price.from_str("90.001"),
                close=Price.from_str("90.003"),
                volume=Quantity.from_int(1_000_000),
                ts_event=0,
                ts_init=0,
            ),
        ]
        with pytest.raises(AssertionError):
            wrangler.validate(bars)

        # assert only one venue
        bars = [
            Bar(
                bar_type=BarType.from_str("AUD/USD.SIM1-1-MINUTE-BID-EXTERNAL"),
                open=Price.from_str("90.002"),
                high=Price.from_str("90.004"),
                low=Price.from_str("90.001"),
                close=Price.from_str("90.003"),
                volume=Quantity.from_int(1_000_000),
                ts_event=0,
                ts_init=0,
            ),
            Bar(
                bar_type=BarType.from_str("AUD/USD.SIM2-1-MINUTE-BID-EXTERNAL"),
                open=Price.from_str("90.002"),
                high=Price.from_str("90.004"),
                low=Price.from_str("90.001"),
                close=Price.from_str("90.003"),
                volume=Quantity.from_int(1_000_000),
                ts_event=0,
                ts_init=0,
            ),
        ]
        with pytest.raises(AssertionError):
            wrangler.validate(bars)

        # assert symbol format
        with pytest.raises(ValueError):
            wrangler.validate([TestDataStubs.bar_5decimal()])

    def _read_bars(self) -> list[Bar]:
        test_data_dir = Path(PACKAGE_ROOT) / "tests/unit_tests/continuous/data"
        paths = list(test_data_dir.glob("HG=*.SIM-1-DAY-MID-EXTERNAL.parquet"))
        session = DataBackendSession()
        for i, path in enumerate(paths):
            assert path.exists()
            session.add_file(NautilusDataType.Bar, f"data{i}", str(path))

        bars = []
        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            bars.extend(chunk)
        return bars

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
