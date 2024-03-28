import numpy as np
import pandas as pd

from pyfutures.continuous.bar import ContinuousBar


def continuous_to_adjusted(bars: list[ContinuousBar]) -> list[float]:
    return [
        (
            bar.current_month.value,
            bar.current_close,
            bar.previous_month.value if bar.previous_month is not None else None,
            bar.previous_close,
        )
        for bar in bars
    ]


def _continuous_to_adjusted(df: pd.DataFrame) -> list[float]:
    # TODO: handle None values
    """
    current_month, current_close, forward_month, forward_close
    creating the adjusted from the continuous bars
    iterate over continuous bars backwards
    when it rolls shift the prices by the adjustment value from the bar after the roll
    """
    # .fillna(False)
    mask = df.current_month != df.current_month.shift(1)
    mask.iloc[0] = False
    values = pd.Series(np.full(len(df), np.nan))
    values.loc[mask] = df.current_price.loc[mask] - df.previous_price.loc[mask]
    values = values.shift(-1).bfill().fillna(0)
    df["adj_value"] = values
    df["adjusted"] = df.current_price + df.adj_value
    return list(df.adjusted)


# class AdjustedPrices:
#     def __init__(
#         self,
#         maxlen: int,
#         bar_type: BarType,
#     ):
#         super().__init__()

#         self.maxlen = maxlen
#         self.values: deque[float] = deque(maxlen=maxlen)

#         self._bar_type = bar_type
#         self._instrument_id = self._bar_type.instrument_id
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

#     def on_continuous_bar(self, bar: ContinuousBar) -> None:
#         if self._last is None or self._last.current_bar.bar_type == bar.current_bar.bar_type:
#             self.values.append(float(bar.close))
#             return

#         adjustment_value = float(bar.current_bar.close) - float(bar.previous_bar.close)
#         self.values = deque([x + adjustment_value for x in self.values], maxlen=self.maxlen)
