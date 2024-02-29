from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
import pandas as pd


def bar_data_to_dict(obj: BarData) -> dict:
    return {
        "timestamp": obj.date,
        "open": obj.open,
        "high": obj.high,
        "low": obj.low,
        "close": obj.close,
        "volume": obj.volume,
        "wap": obj.wap,
        "barCount": obj.barCount,
    }


def parse_datetime(value: str) -> pd.Timestamp:
    if isinstance(value, str):
        assert len(value.split()) != 3, f"datetime value was {value}"
    if isinstance(value, int) or value.isdigit():
        return pd.to_datetime(int(value), unit='s', tz="UTC")
    elif isinstance(value, str) and len(value) == 8:
        return pd.to_datetime(value, format="%Y%m%d", utc=True)  # daily historical bars: YYYYmmdd
    elif isinstance(value, str) and len(value) == 17:
        return pd.to_datetime(value, format="%Y%m%d-%H:%M:%S", utc=True)

    raise RuntimeError("Unable to parse timestamp")

def historical_tick_bid_ask_to_dict(obj: HistoricalTickBidAsk) -> dict:
    return {
        "time": parse_datetime(obj.time),
        "bid": obj.priceBid,
        "ask": obj.priceAsk,
        "bid_size": obj.sizeBid,
        "ask_size": obj.sizeAsk,
    }