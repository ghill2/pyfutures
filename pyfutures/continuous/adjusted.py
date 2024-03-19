from collections import deque

import pandas as pd
from nautilus_trader.common.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import DataType
from nautilus_trader.continuous.bar import ContinuousBar


class AdjustedPrices:
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
        self._last = None

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

    def on_continuous_bar(self, bar: ContinuousBar) -> float | None:
        
        if self._last is None or self._last.current_bar.bar_type == bar.current_bar.bar_type:
            self.values.append(float(bar.close))
            return
        
        adjustment_value = float(bar.current_bar.close) - float(bar.previous_bar.close)
        self.values = deque([x + adjustment_value for x in self.values], maxlen=self.maxlen)