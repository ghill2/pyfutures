import pandas as pd
from pyfutures.continuous.multiple_price import MultiplePrice
from collections import deque
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType

class AdjustedPrices:
    
    def __init__(
        self,
        lookback: int,
        bar_type: BarType,  # bar type that triggers the adjustment
        manual: bool = False,
    ):
        self.lookback = lookback
        self._adjusted_prices = deque(maxlen=self.lookback)
        # self._multiple_prices = deque(maxlen=self._lookback)
        # self.topic = f"{bar_type}a"
        self._bar_type = bar_type
        self._instrument_id = self._bar_type.instrument_id
        self._manual = manual
        self._last = None
    
    @property
    def prices(self) -> deque:
        return self._adjusted_prices
    
    def handle_price(self, price: MultiplePrice) -> float | None:
        
        value = None
        has_rolled = (
            not self._manual
            and self._last is not None
            and self._last.current_month != price.current_month
        )
        
        if has_rolled:
            value = float(price.current_price) - float(self._last.current_price)
            self.adjust(value)
            
        self._adjusted_prices.append(price.current_price)
        self._last = price
        
        return value
    
    def adjust(self, value: float) -> None:
        self._adjusted_prices = deque(
            list(pd.Series(self._adjusted_prices) + value),
            maxlen=self.lookback,
        )
    
    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(
            list(map(MultiplePrice.to_dict, self._multiple_prices))
        )
        df["adjusted"] = list(map(float, self._adjusted_prices))
        df["timestamp"] = list(map(unix_nanos_to_dt, df["ts_event"]))
        return df