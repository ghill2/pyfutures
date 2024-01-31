import pandas as pd
from pyfutures.continuous.price import MultiplePrice
from collections import deque
from copy import copy
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType

class AdjustedPrices:
    
    def __init__(
        self,
        lookback: int,
        bar_type: BarType,  # bar type that triggers the adjustment
        manual: bool = False,
    ):
        self._lookback = lookback
        self._adjusted_prices = deque(maxlen=self._lookback)
        self._multiple_prices = deque(maxlen=self._lookback)
        
        self._bar_type = bar_type
        self._manual = manual
    
    def handle_price(self, price: MultiplePrice) -> float | None:
        
        value = None
        if (
            not self._manual
            and len(self._multiple_prices) > 0
            and self._multiple_prices[-1].current_month != price.current_month
        ):
            
            last = self._multiple_prices[-1]
            if last.forward_price is None or last.current_price is None:
                print(f"No forward or current price found for {price.instrument_id}")
                exit()
            
            value = self.adjustment_value
            self.adjust(value)
            
        self._adjusted_prices.append(price.current_price)
        self._multiple_prices.append(price)
        
        assert len(self._multiple_prices) == len(self._adjusted_prices)
        return value
    
    @property
    def adjustment_value(self) -> float:
        last = self._multiple_prices[-1]
        roll_differential = float(last.forward_price) - float(last.current_price)
        return roll_differential
        
    def adjust(self, value: float) -> None:
        self._adjusted_prices = deque(
            pd.Series(self._adjusted_prices) + value,
            maxlen=self._lookback,
        )
        
    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(
            list(map(MultiplePrice.to_dict, self._multiple_prices))
        )
        df["adjusted"] = list(map(float, self._adjusted_prices))
        df["timestamp"] = list(map(unix_nanos_to_dt, df["ts_event"]))
        return df

    # def to_series(self) -> pd.Series:
    #     return pd.Series(
    #         self._adjusted_prices,
    #         index=map(unix_nanos_to_dt, self._adjusted_timestamps),
    #     )