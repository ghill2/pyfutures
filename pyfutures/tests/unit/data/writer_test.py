from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.data.writer import BarParquetWriter
from pyfutures.data.writer import MultipleBarParquetWriter
from pyfutures.data.writer import QuoteTickParquetWriter

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.multiple_bar import MultipleBar

pytestmark = pytest.mark.skip

class TestParquetWriter:
    def setup(self):
        self.instrument = TestInstrumentProvider.btcusdt_binance()
    
    def test_write_quote_objects_writes_expected(self, tmpdir):
        # Arrange
        quotes = [TestDataStubs.quote_tick(instrument=self.instrument)]

        path = Path(tmpdir) / f"{UUID4()}.parquet"

        writer = QuoteTickParquetWriter(path=path, instrument=self.instrument)

        # Act
        writer.write_objects(quotes)

        assert path.exists()

        # Assert quote ticks are readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.QuoteTick, "data", str(path))

        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            assert QuoteTick.to_dict(chunk[0]) == {
                "type": "QuoteTick",
                "instrument_id": "BTCUSDT.BINANCE",
                "bid_price": "1.00",
                "ask_price": "1.00",
                "bid_size": "100000.000000",
                "ask_size": "100000.000000",
                "ts_event": 0,
                "ts_init": 0,
            }
            break

    def test_write_quote_dataframe_writes_expected(self, tmpdir):
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "bid_price": [1.0],
                "ask_price": [1.1],
                "ask_size": [2.1],
                "bid_size": [2.0],
            },
        )
        orig_df = df.copy()

        path = Path(tmpdir) / f"{UUID4()}.parquet"

        writer = QuoteTickParquetWriter(path=path, instrument=self.instrument)

        # Act
        writer.write_dataframe(df)

        # Assert
        assert path.exists()
        pd.read_parquet(path).equals(df)

        # Assert invalid schema raises
        with pytest.raises(AssertionError):
            writer.write_dataframe(df.reindex(columns=["ask bid timestamp"]))

        # Assert original dataframe is preserved
        pd.testing.assert_frame_equal(orig_df, df)

        # Assert append
        # writer.write_dataframe(df)
        # writer.write_dataframe(df)
        # pd.read_parquet(path).equals(pd.concat([df, df, df]))

        # Assert quote ticks are readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.QuoteTick, "data", str(path))
        result = session.to_query_result()
        for chunk in result:
            chunk = capsule_to_list(chunk)
            assert QuoteTick.to_dict(chunk[0]) == {
                "type": "QuoteTick",
                "instrument_id": "BTCUSDT.BINANCE",
                "bid_price": "1.00",
                "ask_price": "1.10",
                "bid_size": "2.000000",
                "ask_size": "2.100000",
                "ts_event": 1098664387007000000,
                "ts_init": 1098664387007000000,
            }

    def test_write_quote_dataframe_append_writes_expected(self, tmpdir):
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "bid_price": [1.0],
                "ask_price": [1.1],
                "ask_size": [2.1],
                "bid_size": [2.0],
            },
        )

        path = Path(tmpdir) / f"{UUID4()}.parquet"

        writer = QuoteTickParquetWriter(path=path, instrument=self.instrument)

        # Act
        writer.write_dataframe(df, append=False)
        writer.write_dataframe(df, append=True)
        writer.write_dataframe(df, append=True)

        # Assert
        assert len(pd.read_parquet(path)) == 3

        # Assert quote ticks are readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.QuoteTick, "data", str(path))
        result = session.to_query_result()
        for chunk in result:
            chunk = capsule_to_list(chunk)
            assert len(chunk) == 3

    def test_write_quote_dataframe_writes_default_volume(self, tmpdir):
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "bid_price": [1.0],
                "ask_price": [1.1],
            },
        )

        path = Path(tmpdir) / "BTCUSD-BINANCE-FX-QUOTETICK-1-TICK-ASK-2004.parquet"

        writer = QuoteTickParquetWriter(path=path, instrument=self.instrument)

        # Act
        writer.write_dataframe(df)

        # Assert
        session = DataBackendSession()
        session.add_file(NautilusDataType.QuoteTick, "data", str(path))
        result = session.to_query_result()
        for chunk in result:
            chunk = capsule_to_list(chunk)
            assert QuoteTick.to_dict(chunk[0]) == {
                "type": "QuoteTick",
                "instrument_id": "BTCUSDT.BINANCE",
                "bid_price": "1.00",
                "ask_price": "1.10",
                "bid_size": "18446744073.000000",
                "ask_size": "18446744073.000000",
                "ts_event": 1098664387007000000,
                "ts_init": 1098664387007000000,
            }

    def test_write_bar_dataframe_raises_invalid_schema(self, tmpdir):
        # Arrange
        path = Path(tmpdir) / "EURDKK-DUKA-FX-BAR-1-HOUR-BID-2004.parquet"

        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "open": [1],
                "high": [2],
                "low": [3],
                "close": [1],
                "volume": [5],
            },
        )

        bar_type = BarType.from_str("EURDKK.DUKA-1-HOUR-BID-EXTERNAL")
        writer = BarParquetWriter(path=path, instrument=self.instrument, bar_type=bar_type)

        # Act & Arrange
        with pytest.raises(AssertionError):
            writer.write_dataframe(df)

    def test_write_quote_dataframe_raises_invalid_schema(self, tmpdir):
        # Arrange
        path = Path(tmpdir) / "EURDKK-DUKA-FX-QUOTETICK-1-HOUR-BID-2004.parquet"

        writer = QuoteTickParquetWriter(path=path, instrument=self.instrument)

        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "bid_price": [1],
                "ask_price": [1],
                "ask_size": [1],
                "bid_size": [1],
            },
        )

        # Act & Assert
        with pytest.raises(AssertionError):
            writer.write_dataframe(df)

    def test_write_bar_dataframe_writes_default_volume(self, tmpdir):
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "open": [0.1],
                "high": [0.2],
                "low": [0.3],
                "close": [0.4],
            },
        )

        path = Path(tmpdir) / "EURDKK-DUKA-FX-BAR-1-HOUR-BID-2004.parquet"

        writer = BarParquetWriter(
            path=path,
            instrument=self.instrument,
            bar_type=BarType.from_str("EURDKK.DUKA-1-HOUR-BID-EXTERNAL"),
        )

        # Act
        writer.write_dataframe(df)

        # Assert quote ticks are readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.Bar, "data", str(path))

        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            assert Bar.to_dict(chunk[0]) == {
                "type": "Bar",
                "bar_type": "EURDKK.DUKA-1-HOUR-BID-EXTERNAL",
                "open": "0.10",
                "high": "0.20",
                "low": "0.30",
                "close": "0.40",
                "volume": "18446744073.000000",
                "ts_event": 1098664387007000000,
                "ts_init": 1098664387007000000,
            }

    def test_write_bar_dataframe_writes_expected(self, tmpdir):
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "open": [0.1],
                "high": [0.2],
                "low": [0.3],
                "close": [0.4],
                "volume": [0.5],
            },
        )

        orig_df = df.copy()
        path = Path(tmpdir) / "EURDKK-DUKA-FX-BAR-1-HOUR-BID-2004.parquet"

        writer = BarParquetWriter(
            path=path,
            instrument=self.instrument,
            bar_type=BarType.from_str("EURDKK.DUKA-1-HOUR-BID-EXTERNAL"),
        )

        # Act
        writer.write_dataframe(df)

        # Assert quote ticks are readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.Bar, "data", str(path))

        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            assert Bar.to_dict(chunk[0]) == {
                "type": "Bar",
                "bar_type": "EURDKK.DUKA-1-HOUR-BID-EXTERNAL",
                "open": "0.10",
                "high": "0.20",
                "low": "0.30",
                "close": "0.40",
                "volume": "0.500000",
                "ts_event": 1098664387007000000,
                "ts_init": 1098664387007000000,
            }
            break

        # Assert
        assert path.exists()
        pd.read_parquet(path).equals(df)

        # Assert original dataframe is preserved
        pd.testing.assert_frame_equal(orig_df, df)

    def test_write_bar_dataframe_append_writes_expected(self, tmpdir):
        df = pd.DataFrame.from_dict(
            {
                "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
                "open": [0.1],
                "high": [0.2],
                "low": [0.3],
                "close": [0.4],
                "volume": [0.5],
            },
        )

        path = Path(tmpdir) / "EURDKK-DUKA-FX-BAR-1-HOUR-BID-2004.parquet"

        writer = BarParquetWriter(
            path=path,
            instrument=self.instrument,
            bar_type=BarType.from_str("EURDKK.DUKA-1-HOUR-BID-EXTERNAL"),
        )

        # Act
        writer.write_dataframe(df, append=False)
        writer.write_dataframe(df, append=True)
        writer.write_dataframe(df, append=True)

        # Assert
        assert len(pd.read_parquet(path)) == 3

        # Assert readable
        session = DataBackendSession()
        session.add_file(NautilusDataType.Bar, "data", str(path))
        for chunk in session.to_query_result():
            chunk = capsule_to_list(chunk)
            assert len(chunk) == 3

    def test_write_continuous_price_dataframe_writes_expected(self, tmpdir):
        
        # Arrange
        df = pd.DataFrame.from_dict(
            {
                "instrument_id": ["MES.IB"],
                "carry_price": [0.1],
                "carry_id": [7222],
                "current_price": [0.1],
                "current_id": [7222],
                "forward_price": [0.1],
                "forward_id": [7222],
                "expiry_date_ns": [1639526400000000000],
                "roll_date_ns": [1639094400000000000],
                "ts_event": [1602460800000000000],
                "ts_init": [1602460800000000000],
            },
        )

        df.copy()
        path = Path(tmpdir) / "EURDKK-DUKA-1-HOUR-BID-EXTERNAL-CONTINUOUSPRICE.parquet"

        writer = MultipleBarParquetWriter(
            path=path,
            # instrument=self.instrument,
            # bar_type=BarType.from_str("EURDKK.DUKA-1-HOUR-BID-EXTERNAL"),
        )

        # Act
        writer.write_dataframe(df)

        # Assert readable

        for batch in pq.ParquetFile(str(path)).iter_batches():
            [price] = ArrowSerializer.deserialize(data_cls=ContinuousPrice, batch=batch)
            assert price.instrument_id == InstrumentId.from_str("MES.IB")
            assert price.carry_price == 0.1
            assert price.carry_id == ContractMonth.from_int(7222)
            assert price.current_price == 0.1
            assert price.current_id == ContractMonth.from_int(7222)
            assert price.current_price == 0.1
            assert price.current_id == ContractMonth.from_int(7222)
            assert price.expiry_date_ns == 1639526400000000000
            assert price.roll_date_ns == 1639094400000000000
            assert price.ts_event == 1602460800000000000
            assert price.ts_init == 1602460800000000000
    
    @pytest.mark.skip(reason="change to multiple bar")
    def test_write_continuous_price_objects_writes_expected(self, tmpdir):
        # Arrange
        expected = [
            ContinuousPrice(
                instrument_id=TestIdStubs.gbpusd_id(),
                forward_price=Price.from_str("1.0"),
                forward_month=ContractMonth("2021Z"),
                current_price=Price.from_str("1.1"),
                current_month=ContractMonth("2021X"),
                carry_price=Price.from_str("1.0"),
                carry_month=ContractMonth("2021Z"),
                ts_event=0,
                ts_init=0,
            ),
            ContinuousPrice(
                instrument_id=TestIdStubs.gbpusd_id(),
                current_price=Price.from_str("1.2"),
                current_month=ContractMonth("2021X"),
                forward_price=None,
                forward_month=ContractMonth("2021Z"),
                carry_price=None,
                carry_month=ContractMonth("2021Z"),
                ts_event=0,
                ts_init=0,
            ),
        ]
        # quotes = [TestDataStubs.quote_tick(instrument=self.instrument)]

        path = Path(tmpdir) / f"{UUID4()}.parquet"

        writer = MultipleBarParquetWriter(path=path)

        # Act
        writer.write_objects(expected)

        assert path.exists()

        batch = next(pq.ParquetFile(path).iter_batches(batch_size=2))
        deserialized = ArrowSerializer.deserialize(data_cls=ContinuousPrice, batch=batch)
        assert deserialized == expected
        
        # for chunk in session.to_query_result():

        #     chunk = capsule_to_list(chunk)
        #     assert Bar.to_dict(chunk[0]) == {
        #         "type": "Bar",
        #         "bar_type": "EURDKK.DUKA-1-HOUR-BID-EXTERNAL",
        #         "open": "0.10",
        #         "high": "0.20",
        #         "low": "0.30",
        #         "close": "0.40",
        #         "volume": "0.500000",
        #         "ts_event": 1098664387007000000,
        #         "ts_init": 1098664387007000000,
        #     }
        #     break

        # # Assert
        # assert path.exists()
        # pd.read_parquet(path).equals(df)

        # # Assert original dataframe is preserved
        # pd.testing.assert_frame_equal(orig_df, df)
