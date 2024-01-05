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
        2021Z -> ContractId(year=2021, month=12)
        """
        assert isinstance(value, str)
        assert len(value) == 5
        self.year = int(f"{value[:4]}")
        self.letter_month = value[4]
        self.month = letter_month_to_int(value[4])
        self.value = value
        self.timestamp_utc = pd.Timestamp(year=self.year, month=self.month, day=1, tz="UTC")
    
    @classmethod
    def from_year_letter_month(cls, year: int, letter_month: str) -> ContractMonth:
        assert isinstance(year, int)
        assert isinstance(letter_month, str)
        return cls(f"{year}{letter_month}")
    
    @classmethod
    def from_month_year(cls, year: int, month: int) -> ContractMonth:
        assert isinstance(month, int)
        assert month >= 1 and month <= 12
        return cls(f"{year}{int_to_letter_month(month)}")
    
    @classmethod
    def now(cls):
        now = pd.Timestamp.utcnow()
        return cls.from_month_year(year=now.year, month=now.month)
    
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


def letter_month_to_int(letter_month: str) -> int:
    assert letter_month in MONTH_LIST
    return MONTH_LIST.index(letter_month) + 1

def int_to_letter_month(value: int) -> str:
    assert value > 0 and value < 13
    return MONTH_LIST[value - 1]
