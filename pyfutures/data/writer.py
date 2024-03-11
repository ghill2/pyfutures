from __future__ import annotations

from io import BytesIO

# import time
from pathlib import Path

import fastparquet

# from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from nautilus_trader.core.nautilus_pyo3.persistence import DataTransformer
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow

from pyfutures.continuous.Z.multiple_bar import MultipleBar
from pyfutures.data.schemas import BAR_TABLE_SCHEMA
from pyfutures.data.schemas import QUOTE_TABLE_SCHEMA
from pyfutures.data.schemas import DataFrameSchema


class ParquetWriter:
    def __init__(
        self,
        path: Path | str,
    ):
        self._path = Path(path)

    def write_objects(self, objects: list[DataType]) -> None:
        raise NotImplementedError

    def write_dataframe(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def write_table(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def _write_table_with_metadata(
        self,
        table: pa.Table,
        metadata: dict,
        append: bool = False,
    ) -> None:
        # append = self._path.exists() and pq.ParquetFile(self._path).metadata.num_rows > 0
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # causes metadata error v
        # pq.write_table(
        #     table=table,
        #     where=self._path,
        #     row_group_size=500,
        # )
        fastparquet.write(
            self._path,
            table.to_pandas(),
            row_group_offsets=500,
            custom_metadata=metadata,
            append=append,
        )


class BarParquetWriter(ParquetWriter):
    def __init__(
        self,
        path: Path | str,
        bar_type: BarType,
        price_precision: int,
        size_precision: int,
    ):
        super().__init__(path=path)
        self._bar_type = bar_type
        self._price_precision = price_precision
        self._size_precision = size_precision

    def write_dataframe(self, df: pd.DataFrame, append: bool = False) -> None:
        df = DataFrameSchema.validate_bars(df)

        timestamps = pd.to_datetime(df["timestamp"], utc=True, format="mixed").dt.tz_localize(None).view("int64").astype("uint64")
        open = (df["open"] * 1e9).astype("int64")
        high = (df["high"] * 1e9).astype("int64")
        low = (df["low"] * 1e9).astype("int64")
        close = (df["close"] * 1e9).astype("int64")
        volume = (df["volume"] * 1e9).astype("uint64")

        arrays = [
            pa.array(open.values),
            pa.array(high.values),
            pa.array(low.values),
            pa.array(close.values),
            pa.array(volume.values),
            pa.array(timestamps.values),
            pa.array(timestamps.values),
        ]
        table = pa.Table.from_arrays(arrays, schema=BAR_TABLE_SCHEMA)
        self.write_table(table, append=append)

    def write_table(self, table: pa.Table, append: bool = False):
        assert table.schema.remove_metadata().equals(BAR_TABLE_SCHEMA)

        metadata = {
            "bar_type": str(self._bar_type),
            "price_precision": str(self._price_precision),
            "size_precision": str(self._size_precision),
        }
        # table.schema.add_metadata(metadata)
        self._write_table_with_metadata(table=table, metadata=metadata, append=append)


class QuoteTickParquetWriter(ParquetWriter):
    def __init__(
        self,
        path: Path | str,
        instrument_id: InstrumentId,
        price_precision: int,
        size_precision: int,
    ):
        super().__init__(path=path)
        self._instrument_id = instrument_id
        self._price_precision = price_precision
        self._size_precision = size_precision

    def write_objects(self, objects: list[QuoteTick]) -> None:
        batches_bytes = DataTransformer.pyobjects_to_batches_bytes(objects)
        batches_stream = BytesIO(batches_bytes)
        reader = pa.ipc.open_stream(batches_stream)
        table = reader.read_all()
        pq.write_table(table=table, where=str(self._path))
        reader.close()

    def write_dataframe(self, df: pd.DataFrame, append: bool = False) -> None:
        df = DataFrameSchema.validate_quotes(df)

        timestamps = pd.to_datetime(df["timestamp"], utc=True, format="mixed").dt.tz_localize(None).view("int64").astype("uint64")

        bid_price = (df["bid_price"] * 1e9).astype("int64")
        ask_price = (df["ask_price"] * 1e9).astype("int64")
        ask_size = (df["ask_size"] * 1e9).astype("uint64")
        bid_size = (df["bid_size"] * 1e9).astype("uint64")

        arrays = [
            pa.array(bid_price.values),
            pa.array(ask_price.values),
            pa.array(bid_size.values),
            pa.array(ask_size.values),
            pa.array(timestamps.values),
            pa.array(timestamps.values),
        ]

        table = pa.Table.from_arrays(arrays, schema=QUOTE_TABLE_SCHEMA)
        self.write_table(table, append=append)

    def write_table(self, table: pa.Table, append: bool = False):
        assert table.schema.remove_metadata().equals(QUOTE_TABLE_SCHEMA)

        metadata = {
            "instrument_id": str(self._instrument_id),
            "price_precision": str(self._price_precision),
            "size_precision": str(self._size_precision),
        }
        # table.schema.add_metadata(metadata)
        self._write_table_with_metadata(table=table, metadata=metadata, append=append)


class MultipleBarParquetWriter(ParquetWriter):
    def __init__(
        self,
        path: Path | str,
    ):
        super().__init__(path=path)

    def write_dataframe(self, df: pd.DataFrame, append: bool = False) -> None:
        prices = [MultipleBar.from_dict(d) for d in df.to_dict(orient="records")]
        self.write_objects(prices)

    def write_objects(self, data: list[MultipleBar], append: bool = False) -> None:
        register_arrow(
            data_cls=MultipleBar,
            schema=MultipleBar.schema(),
            encoder=make_dict_serializer(schema=MultipleBar.schema()),
            decoder=make_dict_deserializer(data_cls=MultipleBar),
        )
        batch = ArrowSerializer.serialize(data=data, data_cls=MultipleBar)
        self.write_table(pa.Table.from_batches([batch]), append=append)

    @staticmethod
    def to_table(data: list[MultipleBar]) -> pa.Table:
        register_arrow(
            data_cls=MultipleBar,
            schema=MultipleBar.schema(),
            encoder=make_dict_serializer(schema=MultipleBar.schema()),
            decoder=make_dict_deserializer(data_cls=MultipleBar),
        )
        batch = ArrowSerializer.serialize(data=data, data_cls=MultipleBar)
        return batch

    def write_table(self, table: pa.Table, append: bool = False):
        assert table.schema.remove_metadata().equals(MultipleBar.schema())
        self._write_table_with_metadata(table=table, metadata={}, append=append)
