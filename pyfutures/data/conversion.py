from __future__ import annotations

import pandas as pd
import pyarrow as pa

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from pyfutures.core.datetime import unix_nanos_to_dt_vectorized
from pyfutures.data.schemas import BAR_TABLE_SCHEMA
from pyfutures.data.schemas import QUOTE_TABLE_SCHEMA
from pyfutures.data.schemas import TableSchema


def tick_to_bar(
    ticks: pa.Table,
    step: int,
    aggregation: BarAggregation,
):
    ticks = TableSchema.validate_quotes(ticks)

    ticks = ticks.to_pandas().copy()
    ticks.index = unix_nanos_to_dt_vectorized(ticks["ts_event"])
    ticks = ticks.drop(columns=["ts_event", "ts_init"])

    freq = BarSpecification(step, aggregation, PriceType.ASK).timedelta

    df = ticks.groupby(pd.Grouper(freq=freq)).agg(
        {
            "bid_price": "ohlc",
            "ask_price": "ohlc",
            "bid_size": "sum",  # last tick value of the bar will be most accurate for testing
            "ask_size": "sum",
        },
    )
    df = df.dropna()

    if df.empty:
        raise ValueError("Converting resulted in an empty dataframe.")

    timestamps = df.index.to_series().apply(dt_to_unix_nanos)

    arrays = [
        pa.array(df["bid_price"]["open"].values),
        pa.array(df["bid_price"]["high"].values),
        pa.array(df["bid_price"]["low"].values),
        pa.array(df["bid_price"]["close"].values),
        pa.array(df["bid_size"]["bid_size"].values),
        pa.array(timestamps.values),
        pa.array(timestamps.values),
    ]

    bid_table = pa.Table.from_arrays(arrays, schema=BAR_TABLE_SCHEMA)

    arrays = [
        pa.array(df["ask_price"]["open"].values),
        pa.array(df["ask_price"]["high"].values),
        pa.array(df["ask_price"]["low"].values),
        pa.array(df["ask_price"]["close"].values),
        pa.array(df["ask_size"]["ask_size"].values),
        pa.array(timestamps.values),
        pa.array(timestamps.values),
    ]

    ask_table = pa.Table.from_arrays(arrays, schema=BAR_TABLE_SCHEMA)

    data = {
        PriceType.BID: bid_table,
        PriceType.ASK: ask_table,
    }

    return data


def bar_to_bar(
    bars: pd.DataFrame,
    step: int,
    aggregation: BarAggregation,
):
    ohlc_dict = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "first",
    }

    freq = BarSpecification(step, aggregation, PriceType.ASK).timedelta
    return bars.resample(freq, closed="left", label="left").apply(ohlc_dict).dropna()
    




def bar_to_tick(bid_data: pd.DataFrame, ask_data: pd.DataFrame) -> pd.DataFrame:
    assert len(bid_data) == len(ask_data)

    bid_data = bid_data.to_pandas()
    ask_data = ask_data.to_pandas()

    bid_data.index = unix_nanos_to_dt_vectorized(bid_data.ts_event)
    ask_data.index = unix_nanos_to_dt_vectorized(ask_data.ts_event)

    data_open = {
        "bid_price": bid_data["open"],
        "ask_price": ask_data["open"],
        "bid_size": bid_data["volume"],
        "ask_size": ask_data["volume"],
    }

    data_high = {
        "bid_price": bid_data["high"],
        "ask_price": ask_data["high"],
        "bid_size": bid_data["volume"],
        "ask_size": ask_data["volume"],
    }

    data_low = {
        "bid_price": bid_data["low"],
        "ask_price": ask_data["low"],
        "bid_size": bid_data["volume"],
        "ask_size": ask_data["volume"],
    }

    data_close = {
        "bid_price": bid_data["close"],
        "ask_price": ask_data["close"],
        "bid_size": bid_data["volume"],
        "ask_size": ask_data["volume"],
    }

    df_ticks_o = pd.DataFrame(data=data_open)
    df_ticks_h = pd.DataFrame(data=data_high)
    df_ticks_l = pd.DataFrame(data=data_low)
    df_ticks_c = pd.DataFrame(data=data_close)

    # Latency offsets
    df_ticks_o.index = df_ticks_o.index.shift(periods=-300, freq="ms")
    df_ticks_h.index = df_ticks_h.index.shift(periods=-200, freq="ms")
    df_ticks_l.index = df_ticks_l.index.shift(periods=-100, freq="ms")

    # Merge tick data
    df_ticks_final = pd.concat([df_ticks_o, df_ticks_h, df_ticks_l, df_ticks_c])
    df_ticks_final = df_ticks_final.dropna()
    df_ticks_final = df_ticks_final.sort_index(axis=0, kind="mergesort")

    timestamps = df_ticks_final.index.to_series().apply(dt_to_unix_nanos)

    arrays = [
        pa.array(df_ticks_final.bid_price.values),
        pa.array(df_ticks_final.ask_price.values),
        pa.array(df_ticks_final.bid_size.values),
        pa.array(df_ticks_final.ask_size.values),
        pa.array(timestamps.values),
        pa.array(timestamps.values),
    ]

    table = pa.Table.from_arrays(arrays, schema=QUOTE_TABLE_SCHEMA)

    return table


# def quote_rust_to_normal(df: pd.DataFrame) -> pd.DataFrame:
#     ticks = QuoteDataFrameSchema.validate(ticks)
#     df['bid_price'] = df['bid_price'] / 1e9
#     df['ask_price'] = df['ask_price'] / 1e9
#     df['bid_size'] = df['bid_size'] / 1e9
#     df['ask_price'] = df['ask_price'] / 1e9
#     df['timestamp'] = unix_nanos_to_dt_vectorized(df['ts_event'])
#     df.drop("ts_init", inplace=True)
#     df = df[QUOTE_DATAFRAME_SCHEMA.keys()]
#     return df


# interval = _create_interval(spec.aggregation, spec.step)
# assert isinstance(interval, timedelta)
# https://atekihcan.com/blog/codeortrading/changing-timeframe-of-ohlc-candlestick-data-in-pandas/
# TODO df = DataVerify.dataframe_format(df) - don't check for all columns only first ones, filter them if found
# TODO
# add conditional logic to handle transaction data vs quote data.
# transactional data = fillna(0) instead of ffill() for volume
# transaction_data = price = ffill
# print(pa.Table.from_pandas(ticks))

# sampled_df = df.resample( interval ).ohlc() # 1.4GB > 1.7GB (EURUSD 2019)
# se
# https://github.com/peerchemist/finta/blob/fb99f550e17ad0db80d4a5e9cda5381e385759ff/finta/utils.py#L22
# for s in 'open high low close volume'.split():
#     assert s in df.columns, f'{s} column not in dataframe. (case-sensitive)'
# df = df['date open high low close volume'.split()]


# """
#     Remember, finta has timeframe data conversion functionality.

#     ohlc = resample(df, "24h")
#     https://pypi.org/project/finta/

#     """

# class ConversionTask:
#     def __init__(
#         self,
#         specs: list[BarSpecification] = SUPPORTED_SPECIFICATIONS,
#         nrows: int | None = None,
#     ):
#         self._specs = specs
#         self._n_rows = nrows

#     def process(self, ticks: pd.DataFrame) -> dict[BarSpecification, pd.DataFrame]:

#         converted_data = {}

#     for spec in self._specs:

#         df = tick_to_bar(ticks, spec)

#         if df.empty:
#             raise ValueError(f"Converting to spec {spec} resulted in an empty dataframe.")

#         print(f"Processed {spec}...")

#         converted_data[spec] = df

#         return converted_data

# def force_price_type(df, price_type: PriceType) -> pd.DataFrame:
#     s = price_type_to_str(price_type).lower()

#     name_map = {
#         f"{s}_open": "open",
#         f"{s}_high": "high",
#         f"{s}_low": "low",
#         f"{s}_close": "close",
#         f"{s}_volume": "volume",
#     }

#     df = df.rename(name_map, axis=1)[name_map.values()]
#     return df

# def bar_to_bar(
#     bid_bars: pa.Table,
#     ask_bars: pa.Table,
#     aggregation: BarAggregation,
#     step: int,
# ):
#     assert len(bid_bars) == len(ask_bars)

#     bid_bars = bid_bars.to_pandas()
#     ask_bars = ask_bars.to_pandas()

#     df = pd.DataFrame(
#         {
#             "bid_open": bid_bars.open,
#             "bid_high": bid_bars.high,
#             "bid_low": bid_bars.low,
#             "bid_close": bid_bars.close,
#             "bid_volume": bid_bars.volume,
#             "ask_open": ask_bars.open,
#             "ask_high": ask_bars.high,
#             "ask_low": ask_bars.low,
#             "ask_close": ask_bars.close,
#             "ask_volume": ask_bars.volume,
#         },
#     )
#     df.index = unix_nanos_to_dt_vectorized(bid_bars["ts_event"])

#     ohlc_dict = {
#         "bid_open": "first",
#         "bid_high": "max",
#         "bid_low": "min",
#         "bid_close": "last",
#         "bid_volume": "first",
#         "ask_open": "first",
#         "ask_high": "max",
#         "ask_low": "min",
#         "ask_close": "last",
#         "ask_volume": "first",
#     }

#     freq = BarSpecification(step, aggregation, PriceType.ASK).timedelta
#     df = df.resample(freq, closed="left", label="left").apply(ohlc_dict).dropna()

#     timestamps = df.index.to_series().apply(dt_to_unix_nanos)

#     arrays = [
#         pa.array(df.bid_open.values),
#         pa.array(df.bid_high.values),
#         pa.array(df.bid_low.values),
#         pa.array(df.bid_close.values),
#         pa.array(df.bid_volume.values),
#         pa.array(timestamps.values),
#         pa.array(timestamps.values),
#     ]

#     bid_table = pa.Table.from_arrays(arrays, schema=BAR_TABLE_SCHEMA)

#     arrays = [
#         pa.array(df.ask_open.values),
#         pa.array(df.ask_high.values),
#         pa.array(df.ask_low.values),
#         pa.array(df.ask_close.values),
#         pa.array(df.ask_volume.values),
#         pa.array(timestamps.values),
#         pa.array(timestamps.values),
#     ]

#     ask_table = pa.Table.from_arrays(arrays, schema=BAR_TABLE_SCHEMA)

#     data = {
#         PriceType.BID: bid_table,
#         PriceType.ASK: ask_table,
#     }

#     return data