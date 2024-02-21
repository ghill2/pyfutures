import pandas as pd
from nautilus_trader.model.data import DataType
from pyfutures.continuous.multiple_bar import MultipleBar
from collections import deque
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType
from nautilus_trader.common.actor import Actor

class AdjustedPrices(Actor):
    
    def __init__(
        self,
        maxlen: int,
        bar_type: BarType,  # bar type that triggers the adjustment
        manual: bool = False,
    ):
        
        super().__init__()
        
        self.maxlen = maxlen
        self.values: deque[float] = deque(maxlen=maxlen)
        
        self._bar_type = bar_type
        self._instrument_id = self._bar_type.instrument_id
        self._manual = manual
        self._last = None
    
    def on_start(self) -> None:
        self.subscribe_data(DataType(MultipleBar))  # route MultiplePrices to this Actor
        
    def to_series(self) -> pd.Series:
        return pd.Series(list(self.values))
    
    def __len__(self):
        return len(self.values)
    
    def __getitem__(self, index):
        return self.values[index]
    
    def __iter__(self):
        return iter(self.values)
    
    def __next__(self):
        return next(self.values)
        
    def on_multiple_bar(self, price: MultipleBar) -> float | None:
        
        value = None
        has_rolled = (
            not self._manual
            and self._last is not None
            and self._last.current_month != price.current_month
        )
        
        if has_rolled:
            value = float(price.current_bar.close) - float(self._last.current_bar.close)
            self.adjust(value)
            
        self.values.append(float(price.current_bar.close))
        self._last = price
        
        return value
    
    def adjust(self, value: float) -> None:
        self.values = deque(
            [x + value for x in self.values],
            maxlen=self.maxlen
        )
        
    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(
            list(map(MultipleBar.to_dict, self._multiple_prices))
        )
        df["adjusted"] = list(map(float, self._adjusted_prices))
        df["timestamp"] = list(map(unix_nanos_to_dt, df["ts_event"]))
        return df