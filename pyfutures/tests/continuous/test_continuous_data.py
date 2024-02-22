from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock

from nautilus_trader import PACKAGE_ROOT
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.clock import TestClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.objects import Price
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import MultipleData


class TestContinuousData:
    def setup(self):
        self.clock = TestClock()
        self.logger = Logger(
            clock=TestClock(),
            level_stdout=LogLevel.INFO,
            bypass=True,
        )

        self.msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=self.clock,
            logger=self.logger,
        )

        self.cache = Cache(
            logger=self.logger,
        )

        self.data_engine = DataEngine(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            logger=self.logger,
            config=DataEngineConfig(debug=True),
        )

        config = FuturesChainConfig(
            instrument_id="MES.IB",
            hold_cycle="HMUZ",
            priced_cycle="HMUZ",
            roll_offset=-5,
            approximate_expiry_offset=14,
            carry_offset=1,
        )
        chain = FuturesChain(config=config)

        self.bar_type = BarType.from_str("MES.IB-1-DAY-MID-EXTERNAL")
        self.data = MultipleData(
            bar_type=self.bar_type,
            chain=chain,
            start_time_utc=ContractMonth("Z21"),
        )

        self.data.register_base(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            logger=self.logger,
        )

        self.data.start()
        self.data_engine.start()

    def test_sends_expected_price_on_first_bar(self):
        bar = next(self._iterate_bars())

        handler_mock = Mock()

        self.msgbus.subscribe(topic=f"{self.bar_type}0", handler=handler_mock)

        self.data_engine.process(bar)

        assert handler_mock.call_count == 1

        price = handler_mock.call_args[0][0]

        assert price.ts_event == bar.ts_event
        assert price.ts_init == bar.ts_init
        assert price.carry_price is None
        assert price.carry_month == ContractMonth("H22")
        assert price.current_price == Price.from_str("3493.00")
        assert price.current_month == ContractMonth("Z21")
        assert price.forward_price is None
        assert price.forward_month == ContractMonth("H22")

    def test_sends_expected_price_on_roll(self):
        data = []
        self.msgbus.subscribe(topic=f"{self.bar_type}0", handler=data.append)

        for i, bar in enumerate(self._iterate_bars()):
            self.data_engine.process(bar)

            if i == 633:  # before first roll
                assert data[-1].carry_price == Price.from_str("4661.00")
                assert data[-1].carry_month == ContractMonth("H22")
                assert data[-1].current_price == Price.from_str("4712.00")
                assert data[-1].current_month == ContractMonth("Z21")
                assert data[-1].forward_price == Price.from_str("4661.00")
                assert data[-1].forward_month == ContractMonth("H22")
                assert data[-1].ts_event == 1639094400000000000
                assert data[-1].ts_init == 1639094400000000000

            if i == 634:  # after first roll on 2021-12-15 00:00:00+00:00
                assert data[-1].carry_price == Price.from_str("4660.00")
                assert data[-1].carry_month == ContractMonth("M22")
                assert data[-1].current_price == Price.from_str("4704.50")
                assert data[-1].current_month == ContractMonth("H22")
                assert data[-1].forward_price == Price.from_str("4660.00")
                assert data[-1].forward_month == ContractMonth("M22")
                assert data[-1].ts_event == 1639094400000000000
                assert data[-1].ts_init == 1639094400000000000

    def _iterate_bars(self) -> Generator[Bar, None, None]:
        folder = Path(PACKAGE_ROOT) / "tests/test_data/interactive_brokers/data/bar/MES"
        session = DataBackendSession()

        filenames = [
            "MES=H22.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=H23.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=H24.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=M22.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=M23.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=M24.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=U22.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=U23.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=U24.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=Z21.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=Z22.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=Z23.IB-1-DAY-MID-EXTERNAL.parquet",
            "MES=Z24.IB-1-DAY-MID-EXTERNAL.parquet",
        ]

        for i, filename in enumerate(filenames):
            path = folder / filename
            assert path.exists()
            session.add_file(NautilusDataType.Bar, f"data{i}", str(path))

        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            yield from chunk
