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
    if isinstance(value, int):
        # historical ticks int: DDDDDDDDDD
        return pd.to_datetime(value, unit='s', utc=True) 

    # related to request_bars
    # when formatDate=1, timestamps return as 3 parts
    assert len(value.split()) != 3, f"datetime value was {value}"

    if isinstance(value, str) and len(value) == 8:
        # daily historical bars str: YYYYmmdd
        return pd.to_datetime(value, format="%Y%m%d", utc=True)  
    elif isinstance(value, str) and len(value) == 10:
        # < BarSize._1_HOUR historical bars -> str: "DDDDDDDDDD"
        return pd.to_datetime(int(value), unit='s', utc=True)  
    elif isinstance(value, str) and len(value) == 17:
        # formatDate=1
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
