# from collections import deque

# import pandas as pd
# from nautilus_trader.common.actor import Actor
# from nautilus_trader.core.datetime import unix_nanos_to_dt
# from nautilus_trader.model.data import BarType
# from nautilus_trader.model.data import DataType

# from pyfutures.continuous.multiple_bar import MultipleBar


# class AdjustedPrices:
#     def __init__(
#         self,
#         maxlen: int,
#         bar_type: BarType,  # bar type that triggers the adjustment
#         manual: bool = False,
#     ):
#         super().__init__()

#         self.maxlen = maxlen
#         self.values: deque[float] = deque(maxlen=maxlen)

#         self._bar_type = bar_type
#         self._instrument_id = self._bar_type.instrument_id
#         self._manual = manual
#         self._last = None

#     def to_series(self) -> pd.Series:
#         return pd.Series(list(self.values))

#     def __len__(self):
#         return len(self.values)

#     def __getitem__(self, index):
#         return self.values[index]

#     def __iter__(self):
#         return iter(self.values)

#     def __next__(self):
#         return next(self.values)

#     def on_bar(self, bar: Bar) -> float | None:
#         value = None
#         if not self._manual and self._last is not None:
#             value = float(bar.close) - float(self._last.bar.close)
#             self.adjust(value)

#         self.values.append(float(bar.close))
#         return value

#     def roll(self) -> None:
#         value = float(bar.close) - float(self._last.close)
#         self.adjust(value)

#     def adjust(self, value: float) -> None:
#         value = float(bar.close) - float(self._last.bar.close)
#         self.values = deque([x + value for x in self.values], maxlen=self.maxlen)

#     # def to_dataframe(self) -> pd.DataFrame:
#     #     df = pd.DataFrame(list(map(MultipleBar.to_dict, self._multiple_prices)))
#     #     df["adjusted"] = list(map(float, self._adjusted_prices))
#     #     df["timestamp"] = list(map(unix_nanos_to_dt, df["ts_event"]))
#     #     return df
