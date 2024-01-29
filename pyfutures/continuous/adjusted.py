import pandas as pd
from pyfutures.continuous.price import ContinuousPrice
from collections import deque
from copy import copy
from nautilus_trader.core.datetime import unix_nanos_to_dt
class AdjustedPrices:
    
    def __init__(self, lookback: int):
        self._adjusted_prices = deque(maxlen=lookback)
        self._adjusted_timestamps = deque(maxlen=lookback)
        self._last: ContinuousPrice | None = None

    def handle_continuous_price(self, price: ContinuousPrice) -> None:
        
        if self._last is None or self._last.current_month == price.current_month:
            self._adjusted_prices.append(price.current_price)
            self._adjusted_timestamps.append(price.ts_event)
            self._last = price
        elif self._last.current_month != price.current_month:
            if self._last.forward_price is None or self._last.current_price is None:
                print(f"No forward or current price found for {price.instrument_id}")
                exit()
            roll_differential = float(self._last.forward_price) - float(self._last.current_price)
            self._adjusted_prices = deque(list(pd.Series(self._adjusted_prices) + roll_differential))
            self._adjusted_prices.append(price.current_price)
            self._adjusted_timestamps.append(price.ts_event)
            self._last = price
    
    def to_series(self) -> pd.Series:
        return pd.Series(
            self._adjusted_prices,
            index=map(unix_nanos_to_dt, self._adjusted_timestamps),
        )
    