from __future__ import annotations
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession

from nautilus_trader.model.data import capsule_to_list
import os

# import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader.model.data import BarAggregation
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
import pytz
from pyfutures.data.writer import MultipleBarParquetWriter
from pyfutures.data.conversion import bar_to_bar
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.persistence.funcs import urisafe_instrument_id
from pyfutures.core.datetime import unix_nanos_to_dt_vectorized
from pyfutures.data.writer import BarParquetWriter
from pyfutures.data.writer import ParquetWriter
from pyfutures.data.writer import QuoteTickParquetWriter
from pyfutures.continuous.multiple_bar import MultipleBar
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer


from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

def bars_from_rust(df: pd.DataFrame) -> pd.DataFrame:
    df.index = unix_nanos_to_dt_vectorized(df["ts_event"])
    df.open = (df["open"] / 1e9).astype("float64")
    df.high = (df["high"] / 1e9).astype("float64")
    df.low = (df["low"] / 1e9).astype("float64")
    df.close = (df["close"] / 1e9).astype("float64")
    df.volume = (df["volume"] / 1e9).astype("float64")
    df["timestamp"] = unix_nanos_to_dt_vectorized(df.ts_event).rename("timestamp")
    df.drop(["ts_init", "ts_event"], axis=1, inplace=True)
    df = df[["open", "high", "low", "close", "volume", "timestamp"]]
    df.set_index("timestamp", inplace=True)
    return df

def quotes_from_rust(df: pd.DataFrame) -> pd.DataFrame:
    df.index = unix_nanos_to_dt_vectorized(df["ts_event"])
    df.bid = (df["bid"] / 1e9).astype("float64")
    df.ask = (df["ask"] / 1e9).astype("float64")
    df.bid_size = (df["bid_size"] / 1e9).astype("float64")
    df.ask_size = (df["ask_size"] / 1e9).astype("float64")
    df["timestamp"] = unix_nanos_to_dt_vectorized(df.ts_event).rename("timestamp")
    df.drop(["ts_init", "ts_event"], axis=1, inplace=True)
    df = df[["bid", "ask", "bid_size", "ask_size", "timestamp"]]
    df.set_index("timestamp", inplace=True)
    return df

class ParquetFile:
    EXTENSION = ".parquet"

    def __init__(
        self,
        parent: Path | str,
        bar_type: BarType,
        cls: DataType,
        year: int = 0,
    ):
        self.parent = parent
        self.bar_type = bar_type
        self.cls = cls
        self.year = int(year)
    
    @property
    def contract_month(self) -> ContractMonth:
        return ContractMonth(self.bar_type.symbol.value.split("=")[1])
    
    @property
    def instrument_id(self) -> InstrumentId:
        return self.bar_type.instrument_id

    @property
    def symbol(self) -> Symbol:
        return self.bar_type.instrument_id.symbol

    @property
    def venue(self) -> Venue:
        return self.bar_type.instrument_id.venue

    @property
    def spec(self) -> BarSpecification:
        return self.bar_type.spec

    @property
    def step(self) -> int:
        return self.bar_type.spec.step

    @property
    def aggregation(self) -> BarAggregation:
        return self.bar_type.spec.aggregation

    @property
    def price_type(self) -> PriceType:
        return self.bar_type.spec.price_type

    def with_parent(self, parent: str) -> ParquetFile:
        return self.from_path(Path(parent) / self.path.name)

    def with_cls(self, cls: type) -> ParquetFile:
        self.cls = cls
        return self

    def with_spec(self, spec: BarSpecification) -> ParquetFile:
        self.bar_type = BarType.from_str(f"{self.bar_type.instrument_id}-{spec}-EXTERNAL")
        return self

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self):
        return str(self)

    def read(
        self,
        timestamp_delta: tuple[pd.Timedelta, pytz.timezone] | None = None,
        to_aggregation: tuple[int, BarAggregation] | None = None,
        nrows: int | None = None,
        bar_to_quote: bool = False,
    ) -> pd.DataFrame:
        
        if nrows is None:
            nrows = pq.ParquetFile(self.path).metadata.num_rows
        assert isinstance(nrows, int)
        
        for batch in pq.ParquetFile(str(self)).iter_batches(batch_size=nrows):
            df = batch.to_pandas()
            
        if list(df.columns) == ["open", "high", "low", "close", "volume", "ts_event", "ts_init"]:
            df = bars_from_rust(df)
            if to_aggregation is not None:
                step, aggregation = to_aggregation
                df = bar_to_bar(
                    bars=df,
                    step=step,
                    aggregation=aggregation,
                )
            if bar_to_quote:
                df = pd.DataFrame(
                    {
                        "bid": df.close.values,
                        "ask": df.close.values,
                        "bid_size": df.volume.values,
                        "ask_size": df.volume.values,
                        "timestamp": df.index,
                    }
                )
                # df.timestamp = df.timestamp.tz_convert("UTC")
                df.set_index("timestamp", inplace=True)
                
                
        elif list(df.columns) == ["bid", "ask", "bid_size", "ask_size", "ts_event", "ts_init"]:
            df = quotes_from_rust(df)
            # TODO: add quote sampleing
        
        if timestamp_delta is not None:
            delta, timezone = timestamp_delta
            df.index = (df.index.tz_localize(None) + delta + pd.Timedelta(seconds=30))
            df.index = df.index.tz_localize(timezone)
            df.index = df.index.tz_convert("UTC")
            
        return df
    
    def read_objects(self, nrows: int | None = None) -> pd.DataFrame:
        
        session = DataBackendSession()

        if self.cls is Bar or self.cls is QuoteTick:
            
            nautilus_type = ParquetDataCatalog._nautilus_data_cls_to_data_type(self.cls)
            
            session.add_file(nautilus_type, "data", str(self.path))
            data = []
            
            for chunk in session.to_query_result():
                data.extend(capsule_to_list(chunk))
            
            assert len(data) > 0
            return data
        
        elif self.cls is MultipleBar:
            
            
            
            prices = []
            for batch in pq.ParquetFile(str(self)).iter_batches():
                deserialized = ArrowSerializer.deserialize(data_cls=MultipleBar, batch=batch)
                prices.extend(deserialized)
                
            assert len(prices) > 0
            return prices
                
    @property
    def num_rows(self) -> int:
        return pq.ParquetFile(str(self)).metadata.num_rows

    def writer(
        self,
        instrument: Instrument,
    ) -> ParquetWriter:
        if self.cls is Bar:
            return BarParquetWriter(path=self.path, instrument=instrument, bar_type=self.bar_type)
        elif self.cls is QuoteTick:
            return QuoteTickParquetWriter(path=self.path, instrument=instrument)
        elif self.cls is MultipleBar:
            return MultipleBarParquetWriter(path=self.path, instrument=instrument)
        else:
            raise RuntimeError(f"Writer for cls {self.cls} not supported")

    @staticmethod
    def _str_to_cls(value: str) -> DataType:
        if value.lower() == "quotetick":
            return QuoteTick
        elif value.lower() == "bar":
            return Bar
        elif value.lower() == "multipleprice":
            return MultipleBar
        else:
            raise RuntimeError(f"Incompatible type {value}")

    @classmethod
    def from_path(cls, path: Path | str) -> ParquetFile:
        path = Path(path)
        parts = path.stem.split("-")

        bar_type = BarType.from_str("-".join(parts[:5]))

        return cls(
            parent=path.parent,
            cls=cls._str_to_cls(parts[5]),
            bar_type=bar_type,
            year=int(parts[6]),
        )

    @property
    def path(self) -> Path:
        parts = [
            urisafe_instrument_id(str(self.bar_type)),
            self.cls.__name__,
            str(self.year),
        ]
        filename = "-".join(parts).upper() + self.EXTENSION
        return Path(os.path.join(self.parent, filename))


# class ContractDataFile(DataFile):
#     @classmethod
#     def from_path(cls, path: Path | str) -> ContractDataFile:
#         path = Path(path)
#         parts = path.stem.split("-")

#         bar_type = BarType.from_str("-".join(parts[:7]))
#         print(bar_type)

#         return cls(
#             parent=path.parent,
#             cls=cls._str_to_cls(parts[7]),
#             bar_type=bar_type,
#         )

#     @property
#     def path(self) -> Path:
#         parts = [
#             urisafe_instrument_id(str(self.bar_type)),
#             self.cls.__name__,
#         ]
#         filename = "-".join(parts).upper() + self.EXTENSION
#         return Path(os.path.join(self.parent, filename))

#     def read(self) -> pd.DataFrame:
#         df = pd.read_parquet(self.path)
#         return bars_rust_to_normal(df)

#     def read_prices(self) -> pd.Series:
#         df = self.read()
#         data = pd.Series(df.close.rename("prices"))
#         data.index = df.timestamp
#         return data


# class ContractParquetFile(ContractDataFile):
#     EXTENSION = ".parquet"


# class ContractCsvFile(ContractDataFile):
#     EXTENSION = ".csv"


# class YearlyParquetFile(YearlyDataFile):
#     EXTENSION = ".parquet"


# class YearlyCsvFile(YearlyDataFile):
#     EXTENSION = ".csv"


# class AdjustedFile(FuturesData):
#     _type = ParquetFileType.ADJUSTED

# class MultipleFile(FuturesData):
#     _type = ParquetFileType.MULTIPLE

# class ContractFile(ParquetFile):
#     _type = ParquetFileType.CONTRACT

#     def __init__(
#         self,
#         parent: Union[Path, str],
#         cls: type,
#         bar_type: BarType,
#         expiry_date: pd.Timestamp,
#         contract_id: int,
#     ):
#         super().__init__(parent=parent, bar_type=bar_type, cls=cls)
#         self.expiry_date = expiry_date
#         self.contract_id = contract_id

#     @classmethod
#     def from_path(cls, path: Path | str) -> FuturesData:
#         path = Path(path)
#         parts = path.stem.split("-")
#         expiry_date = parts[7]
#         contract_id = parts[8]

#         return cls(
#             parent=path.parent,
#             cls=cls._parse_data_cls(path),
#             bar_type=cls._parse_bar_type(path),
#             expiry_date=pd.to_datetime(expiry_date, format="%Y%m%d"),
#             contract_id=int(contract_id),
#         )

#     @property
#     def path(self) -> Path:
#         parts = [
#             self.instrument_id.value.replace(".", "-"),
#             self._type.name,
#             self.cls.__name__,
#             str(self.spec),
#             self.expiry_date.strftime("%Y%m%d"),
#             str(self.contract_id),
#         ]
#         filename = "-".join(parts).upper() + ".parquet"
#         return Path(os.path.join(self.parent, filename))

# def with_price_type(self, price_type: PriceType) -> ParquetFile:
#     return self._class__(
#         parent=self.parent,
#         cls=self.cls,
#         bar_type=BarType.from_str(f"{self.bar_type.instrument_id}-{spec}-EXTERNAL"),
#         year=self.year,
#         asset_class=self.asset_class,
#     )


# def read_dataframe(self) -> pd.DataFrame:
#     """
#     batches_bytes = DataTransformer.pyo3_quote_ticks_to_batches_bytes(ticks)
#     batches_stream = BytesIO(batches_bytes)
#     reader = pa.ipc.open_stream(batches_stream)

#     assert len(ticks) == 100_000
#     assert len(reader.read_all()) == len(ticks)
#     """
#     return pd.read_parquet(self.path)

# def read_objects(self):
#     """
#     # Arrange
#     path = TEST_DATA_DIR / "truefx-audusd-ticks.csv"
#     df: pd.DataFrame = pd.read_csv(path)

#     # Act
#     wrangler = QuoteTickDataWrangler(AUDUSD_SIM)
#     ticks = wrangler.from_pandas(df)

#     cython_ticks = QuoteTick.from_pyo3(ticks)
#     """

# @classmethod
# def from_path(cls, path: Union[Path, str]) -> ForexFile | AdjustedFile:
#     path = Path(path)
#     parts = path.stem.split("-")
#     _type = parts[2]

#     if _type == ParquetFileType.ADJUSTED.name:
#         return AdjustedFile.from_path(path)
#     if _type == ParquetFileType.FX.name:
#         return ForexFile.from_path(path)
#     else:
#         raise RuntimeError("Invalid parquet file type")

# @property
# def path(self) -> Path:
#     raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover
# class ParquetFileType(Enum):
#     ADJUSTED = 1
#     CONTRACT = 2
#     MULTIPLE = 3
#     FX = 4
#     EQUITY = 5
