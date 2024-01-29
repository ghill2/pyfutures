import pandas as pd
from pyfutures.continuous.price import ContinuousPrice
from collections import deque
from copy import copy

class AdjustedPrices:
    
    def __init__(self, lookback: int):
        self._adjusted = deque(maxlen=lookback)
        self._last: ContinuousPrice | None = None

    def handle_continuous_price(self, price: ContinuousPrice) -> None:
        
        self._adjusted.append(price.current_price)
        
        if self._last is None or self._last.current_month == price.current_month:
            self._adjusted.append(price.current_price)
            self._last = price
        elif self._last.current_month != price.current_month:
            if self._last.forward_price is None or self._last.current_price is None:
                print(f"No forward or current price found for {price.instrument_id}")
                exit()
            roll_differential = float(self._last.forward_price) - float(self._last.current_price)
            self._adjusted = deque(list(pd.Series(self._adjusted) + roll_differential))
            self._adjusted.append(price.current_price)
            self._last = price
    
    def to_series(self) -> pd.Series:
        return pd.Series(copy(list(self._adjusted)))
    