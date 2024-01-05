from __future__ import annotations

import pandas as pd


MONTH_LIST = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
"""
F = Jan
G = Feb
H = March
J = April
K = May
M = June
N = July
Q = AUgust
U = September
V = October
X = November
Z = December
"""

class ContractMonth:
    def __init__(
        self,
        value: str,
    ):
        """
        Z21 -> ContractId(year=2021, month=12)
        """
        assert isinstance(value, str)
        assert len(value) == 3

        if int(value[1:]) > 50:
            year = int(f"19{value[1:]}")
        else:
            year = int(f"20{value[1:]}")

        self.year = year
        self.month = letter_month_to_int(value[0])
        self.letter_month = value[0]
        self.value = value

    @property
    def timestamp_utc(self) -> pd.Timestamp:
        return pd.Timestamp(year=self.year, month=self.month, day=1, tz="UTC")
    
    @classmethod
    def from_year_letter_month(cls, year: int, letter_month: int) -> ContractMonth:
        return cls(f"{letter_month}{str(year)[2:]}")
    
    @classmethod
    def from_month_year(cls, year: int, month: int) -> ContractMonth:
        assert isinstance(year, int)
        assert year > 1950
        assert isinstance(month, int)
        assert month >= 1 and month <= 12
        letter_month = int_to_letter_month(month)
        return cls(f"{letter_month}{str(year)[2:]}")

    @classmethod
    def from_int(cls, value: int) -> ContractMonth:
        return cls.from_month_year(
            year=int(str(value)[:4]),
            month=int(str(value)[4:6]),
        )

    def to_int(self) -> int:
        return int(f"{self.year}{self.month:02d}")

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __str__(self) -> str:
        return self.value


def letter_month_to_int(letter_month: str) -> int:
    assert letter_month in MONTH_LIST
    return MONTH_LIST.index(letter_month) + 1


def int_to_letter_month(value: int) -> str:
    assert value > 0 and value < 13
    return MONTH_LIST[value - 1]
