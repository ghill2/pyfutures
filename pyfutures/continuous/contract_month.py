from __future__ import annotations

from typing import Annotated

import pandas as pd
from msgspec import Meta


LETTER_MONTHS = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

# An integer constrained to values <= 0
NonPositiveInt = Annotated[int, Meta(le=0)]


class ContractMonth:
    def __init__(
        self,
        value: str,
    ):
        """
        2021Z -> ContractId(year=2021, month=12)
        """
        assert isinstance(value, str)
        assert len(value) == 5
        self.year = int(f"{value[:4]}")
        self.letter_month = value[4]
        self.month = letter_month_to_int(value[4])
        self.value = value
        self.timestamp_utc = pd.Timestamp(
            year=self.year, month=self.month, day=1, tz="UTC"
        )

    @classmethod
    def from_month_year(cls, year: int, month: int) -> ContractMonth:
        assert isinstance(month, int)
        assert month >= 1 and month <= 12
        return cls(f"{year}{int_to_letter_month(month)}")

    @classmethod
    def from_int(cls, value: int) -> ContractMonth:
        return cls.from_month_year(
            year=int(str(value)[:4]),
            month=int(str(value)[4:6]),
        )

    @classmethod
    def now(cls) -> ContractMonth:
        now = pd.Timestamp.utcnow()
        return cls.from_month_year(year=now.year, month=now.month)

    def to_int(self) -> int:
        return int(f"{self.year}{self.month:02d}")

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __gt__(self, other) -> bool:
        return self.timestamp_utc > other.timestamp_utc

    def __lt__(self, other) -> bool:
        return self.timestamp_utc < other.timestamp_utc

    def __ge__(self, other) -> bool:
        return self.timestamp_utc >= other.timestamp_utc

    def __le__(self, other) -> bool:
        return self.timestamp_utc <= other.timestamp_utc

    def __str__(self) -> str:
        return self.value

    def __getstate__(self):
        return (
            self.year,
            self.letter_month,
            self.month,
            self.value,
            self.timestamp_utc,
        )

    def __setstate__(self, state):
        self.year = state[0]
        self.letter_month = state[1]
        self.month = state[2]
        self.value = state[3]
        self.timestamp_utc = state[4]


def letter_month_to_int(letter_month: str) -> int:
    assert letter_month in LETTER_MONTHS
    return LETTER_MONTHS.index(letter_month) + 1


def int_to_letter_month(value: int) -> str:
    assert value > 0 and value < 13
    return LETTER_MONTHS[value - 1]
