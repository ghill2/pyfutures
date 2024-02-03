import numpy as np
import pandas as pd
import pyarrow as pa
from numpy.dtypes import Float64DType
from pandas.core.dtypes.dtypes import DatetimeTZDtype

from nautilus_trader.model.objects import QUANTITY_MAX
from pyfutures.pyfutures.continuous.multiple_price import MultiplePrice


DEFAULT_VOLUME = float(QUANTITY_MAX)


# use f.dtypes.to_dict() to get schema for a dataframe
class DataFrameSchema:
    @classmethod
    def validate_bars(cls, df: pd.DataFrame) -> pd.DataFrame:
        if "volume" not in df.columns:
            df["volume"] = DEFAULT_VOLUME

        expected = {
            "timestamp": [DatetimeTZDtype(tz="UTC")],
            "open": [Float64DType(), np.float64],
            "high": [Float64DType(), np.float64],
            "low": [Float64DType(), np.float64],
            "close": [Float64DType(), np.float64],
            "volume": [Float64DType(), np.float64],
        }
        return cls._validate(df, expected)

    @classmethod
    def validate_quotes(cls, df: pd.DataFrame) -> pd.DataFrame:
        if "ask_size" not in df.columns:
            df["ask_size"] = DEFAULT_VOLUME
        if "bid_size" not in df.columns:
            df["bid_size"] = DEFAULT_VOLUME

        expected = {
            "timestamp": [DatetimeTZDtype(tz="UTC")],
            "bid_price": [Float64DType(), np.float64],
            "ask_price": [Float64DType(), np.float64],
            "bid_size": [Float64DType(), np.float64],
            "ask_size": [Float64DType(), np.float64],
        }

        return cls._validate(df, expected)

    @staticmethod
    def _validate(df: pd.DataFrame, expected: dict) -> pd.DataFrame:
        df = df.reset_index().reindex(columns=list(expected.keys()))

        # keys = list(expected.keys())
        # keys.remove("timestamp")
        # for key in keys:
        # assert (df[key] >= 0).all()
        # assert not df[key].isna().any()

        # datetime64[us, UTC] >  datetime64[ns, UTC] safe cast
        if str(df["timestamp"].dtype) == "datetime64[us, UTC]":
            df["timestamp"] = df["timestamp"].astype("datetime64[ns, UTC]")

        # for key in expected.keys():
        #     if key == "timestamp":
        #         continue
        #     if type(df[key].dtype) is np.dtypes.Float64DType:
        #         df[key] = df[key].astype("float64")
        # input_dtypes = {key: dtype for key, dtype in df.dtypes.to_dict().items()}

        for key, dtypes in expected.items():
            assert any(
                df[key].dtype == dtype for dtype in dtypes
            ), f"""
                Dataframe validation failed:
                input: {df.dtypes.to_dict()}
                expected: {expected}
                """

        return df


class TableSchema:
    @classmethod
    def validate_quotes(cls, table: pa.Table) -> pa.Table:
        return cls._validate(table, QUOTE_TABLE_SCHEMA)

    @classmethod
    def validate_bars(cls, table: pa.Table) -> pa.Table:
        return cls._validate(table, BAR_TABLE_SCHEMA)

    @classmethod
    def validate_continuous_prices(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate(df, MultiplePrice.schema())

    @classmethod
    def _validate(cls, table: pa.Table, expected: pa.Schema) -> pa.Table:
        assert table.schema.remove_metadata().equals(expected)
        return table


QUOTE_TABLE_SCHEMA = pa.schema(
    [
        pa.field("bid_price", pa.int64()),
        pa.field("ask_price", pa.int64()),
        pa.field("bid_size", pa.uint64()),
        pa.field("ask_size", pa.uint64()),
        pa.field("ts_event", pa.uint64()),
        pa.field("ts_init", pa.uint64()),
    ],
)

BAR_TABLE_SCHEMA = pa.schema(
    [
        pa.field("open", pa.int64()),
        pa.field("high", pa.int64()),
        pa.field("low", pa.int64()),
        pa.field("close", pa.int64()),
        pa.field("volume", pa.uint64()),
        pa.field("ts_event", pa.uint64()),
        pa.field("ts_init", pa.uint64()),
    ],
)


# BAR_SCHEMA = {
#     "open": np.float64,
#     "high": np.float64,
#     "low": np.float64,
#     "close": np.float64,
#     "volume": np.float64,
#     "timestamp": DatetimeTZDtype(tz="UTC"),
# }

# QUOTETICK_SCHEMA_RUST = {
#     "bid": np.int64,
#     "ask": np.int64,
#     "bid_size": np.uint64,
#     "ask_size": np.uint64,
#     "ts_event": np.uint64,
#     "ts_init": np.uint64,
# }


# # ON READ
# # STORED FORMAT
# QUOTETICK_SCHEMA = {
#     "timestamp": DatetimeTZDtype(tz="UTC"),
#     "bid": np.float64,
#     "ask": np.float64,
#     "bid_size": np.float64,
#     "ask_size": np.float64,
# }


# from nautilus_trader.model.enums import AssetClass
# import pyarrow as pa
# import numpy as np

# from pandas.core.dtypes.common import DT64NS_DTYPE

# from pandas.core.dtypes.dtypes import DatetimeTZDtype
# from nautilus_trader.model.data import QuoteTick
# from nautilus_trader.model.data import Bar

# _DUKA_TIMESTAMP_FORMAT = "%Y.%m.%d %H:%M:%S.%f"


# NAME_MAPS = {
#     "DUKA": {
#         AssetClass.FX: {
#             "QuoteTick": {
#                 "bid": "Bid",
#                 "ask": "Ask",
#                 "bid_size": "BidVolume",
#                 "ask_size": "AskVolume",
#                 "timestamp": "Time (UTC)",
#             },
#             "Bar": {
#                 "open": "open",
#                 "high": "high",
#                 "low": "low",
#                 "close": "close",
#                 "volume": "volume",
#                 "timestamp": "timestamp",
#             },
#         }
#     }
# }

# 1 method: write Objects, QuoteTicks in rust format, Bars in normal format
# 1 method: writes dataframes, QuoteTicks in rust format, Bars in normal format.

# create function to convert quoteticks to normal format, then use write dataframe method

# inside ticks, raw values are integers
# to convert integers to floats, get each field with tick.bid()..then call __float__ on it.
# loop over ticks, create dictionary of the tick with the float convered values.
# then pd.from_records()
# use in write_objects records
# then self.write_dataframe()


# use to_raw_c function in tick.pyx
# delete pyx to_raw

# in write_dataframe method

# processed schemas
# PROC_SCHEMAS = {Bar: BAR_SCHEMA, QuoteTick: QUOTETICK_SCHEMA_RUST}


# QUOTETICK_SCHEMA_RUST = pa.schema()
# schema = pa.schema(
#             {
#                 'bid': pa.int64(),
#                 'ask': pa.int64(),
#                 'ask_size': pa.uint64(),
#                 'bid_size': pa.uint64(),
#                 'ts_event': pa.uint64(),
#                 'ts_init': pa.uint64(),
#             }
#         )

# reading from src parquet -> always normal format
# writing to disk -> rust format for QuoteTicks, normal for bars
