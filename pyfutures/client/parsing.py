from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
import pandas as pd

class ClientParser:

    @classmethod
    def bar_data_to_dict(cls, obj: BarData) -> dict:
        return {
            "timestamp": cls.parse_datetime(obj.date),
            "date": obj.date,
            "open": obj.open,
            "high": obj.high,
            "low": obj.low,
            "close": obj.close,
            "volume": obj.volume,
            "wap": obj.wap,
            "barCount": obj.barCount,
        }
    
    @classmethod
    def bar_data_from_dict(cls, obj: dict) -> dict:
        bar = BarData()
        bar.timestamp = obj["timestamp"]
        bar.date = obj["date"]
        bar.open = obj["open"]
        bar.high = obj["high"]
        bar.low = obj["low"]
        bar.close = obj["close"]
        bar.volume = obj["volume"]
        bar.wap = obj["wap"]
        bar.barCount = obj["barCount"]
        return bar
    
    @classmethod
    def historical_tick_bid_ask_to_dict(cls, obj: HistoricalTickBidAsk) -> dict:
        return {
            "timestamp": cls.parse_datetime(obj.time),
            "time": obj.time,
            "bid": obj.priceBid,
            "ask": obj.priceAsk,
            "bid_size": obj.sizeBid,
            "ask_size": obj.sizeAsk,
        }
    
    @staticmethod
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