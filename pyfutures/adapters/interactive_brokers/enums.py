from __future__ import annotations

from enum import Enum

import pandas as pd
from nautilus_trader.model.data import BarAggregation
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import bar_aggregation_to_str


class WhatToShow(Enum):
    TRADES = "TRADES"
    MIDPOINT = "MIDPOINT"
    BID = "BID"
    ASK = "ASK"
    BID_ASK = "BID_ASK"

    def to_price_type(self):
        assert self.value != "BID_ASK"
        reversed_map = {value: key for key, value in self._map().items()}
        return reversed_map[self]

    @classmethod
    def from_price_type(cls, value: PriceType):
        return cls._map()[value]

    @staticmethod
    def _map() -> dict:
        return {
            PriceType.ASK: WhatToShow.ASK,
            PriceType.BID: WhatToShow.BID,
            PriceType.LAST: WhatToShow.TRADES,
            PriceType.MID: WhatToShow.MIDPOINT,
        }


class Frequency(Enum):
    SECOND = 1
    MINUTE = 2
    HOUR = 3
    DAY = 4
    MONTH = 5
    WEEK = 6
    YEAR = 7


class Duration:
    def __init__(self, step: int, freq: Frequency):
        if freq == Frequency.HOUR or freq == Frequency.MINUTE:
            raise RuntimeError("IB does not support Hourly or MINUTE duration frequency.")

        self.step = step
        self.freq = freq
        self.value = f"{self.step} {self.freq.name[0]}"

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.value

    def to_timedelta(self) -> pd.Timedelta:
        if self.freq == Frequency.SECOND:
            return pd.Timedelta(seconds=self.step)
        elif self.freq == Frequency.MINUTE:
            return pd.Timedelta(minutes=self.step)
        elif self.freq == Frequency.DAY:
            return pd.Timedelta(days=self.step)
        elif self.freq == Frequency.WEEK:
            return pd.Timedelta(days=self.step * 7)
        else:
            raise RuntimeError(f"Timedelta parsing error: the Duration {self} is not a fixed length of time.")


class BarSize(Enum):
    _1_SECOND = (1, Frequency.SECOND)
    _5_SECOND = (5, Frequency.SECOND)
    _15_SECOND = (15, Frequency.SECOND)
    _30_SECOND = (30, Frequency.SECOND)
    _1_MINUTE = (1, Frequency.MINUTE)
    _2_MINUTE = (2, Frequency.MINUTE)
    _3_MINUTE = (3, Frequency.MINUTE)
    _5_MINUTE = (5, Frequency.MINUTE)
    _15_MINUTE = (15, Frequency.MINUTE)
    _30_MINUTE = (30, Frequency.MINUTE)
    _1_HOUR = (1, Frequency.HOUR)
    _1_DAY = (1, Frequency.DAY)



    @property
    def step(self) -> int:
        return self.value[0]

    @property
    def frequency(self) -> Frequency:
        return self.value[1]

    def __str__(self) -> str:
        key = self.frequency.name.lower()
        if self.frequency == Frequency.SECOND or self.frequency == Frequency.MINUTE:
            key = key[:3]

        return f"{self.step} {key}{'' if self.step == 1 else 's'}"

    def to_duration(self) -> Duration:
        if self.frequency == Frequency.MINUTE:
            return Duration(step=self.step * 60, freq=Frequency.SECOND)
        else:
            return Duration(step=self.value[0], freq=self.frequency)

    @classmethod
    def from_bar_spec(cls, bar_spec: BarSpecification) -> BarSpecification:
        valid_steps = {
                BarAggregation.SECOND: (1, 5, 15, 30),
                BarAggregation.MINUTE: (1, 2, 3, 5, 15, 30),
                BarAggregation.HOUR: (1,),
                BarAggregation.DAY: (1,),
        }

        step = bar_spec.step
        aggregation = bar_spec.aggregation
        if step not in valid_steps[aggregation]:
            raise ValueError(
                f"InteractiveBrokers doesn't support subscription for {bar_spec!r}",
            )
        frequency = Frequency[bar_aggregation_to_str(aggregation)]
        return cls((step, frequency))

    # def to_duration(self) -> Duration:
    #     # special handling for HOUR and MINUTE because no DurationStr exists for them
    #     if self.frequency == Frequency.HOUR:
    #         return Duration(3600, Frequency.SECOND)
    #     elif self.frequency == Frequency.MINUTE:
    #         return Duration(self.step * 60, Frequency.SECOND)
    #     else:
    #         return Duration(self.step, self.frequency)
