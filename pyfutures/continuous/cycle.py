from __future__ import annotations


import pandas as pd

from pyfutures.continuous.contract_month import ContractMonth

class RollCycle:
    def __init__(self, value: str, skip_months: list[ContractMonth] | None = None):
        assert isinstance(value, str)

        self.value = "".join(sorted(value))

        self._skip_months = skip_months or []

    def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
        month = ContractMonth.from_month_year(timestamp.year, timestamp.month)

        letter_month = month.letter_month

        if letter_month not in self.value:
            month = self.next_month(month)

        while month.value in self._skip_months:
            month = self.next_month(current=month)

        return month

    def next_month(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        if letter_month not in self.value:
            return self._closest_next(current)

        assert letter_month in self.value

        is_last_month = self.value[-1] == letter_month
        if is_last_month:
            year += 1
            month = self.value[0]
        else:
            month = self.value[self.value.index(letter_month) + 1]

        month = ContractMonth(f"{year}{month}")

        while month in self._skip_months:
            month = self.next_month(current=month)

        return month

    def previous_month(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        if letter_month not in self.value:
            return self._closest_previous(current)

        assert letter_month in self.value

        is_first_month = self.value[0] == letter_month
        if is_first_month:
            year -= 1
            month = self.value[-1]
        else:
            month = self.value[self.value.index(letter_month) - 1]

        month = ContractMonth(f"{year}{month}")

        while month in self._skip_months:
            month = self.previous_month(current=month)

        return month

    def _closest_previous(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        assert letter_month not in self.value
        for char in self.value[::-1]:
            if char < letter_month:
                return ContractMonth(f"{year}{char}")

        year -= 1
        return ContractMonth(f"{year}{self.value[-1]}")

    def _closest_next(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        assert letter_month not in self.value

        for char in self.value:
            if char > letter_month:
                return ContractMonth(f"{year}{char}")

        year += 1
        return ContractMonth(f"{year}{self.value[0]}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __len__(self) -> int:
        return len(self.value)

    def __eq__(self, other):
        if not isinstance(other, RollCycle):
            return False
        return self.value == other.value

    def __str__(self) -> str:
        return self.value

    def __contains__(self, month: ContractMonth) -> bool:
        return month.letter_month in self.value

    def __hash__(self) -> int:
        return hash(self.value)

    def iterate(self, start: ContractMonth, end: ContractMonth, direction: int):
        if direction == 1:
            if start.letter_month not in self.value:
                start = self._closest_next(start)
            while start < end:
                yield start
                start = self.next_month(start)
        if direction == -1:
            raise NotImplementedError  # TODO

    def __getstate__(self):
        return (self.value, self._skip_months)

    def __setstate__(self, state):
        self.value = state[0]
        self._skip_months = state[1]


# from abc import ABC, abstractmethod

# class CycleIterator(ABC):

#     @abstractmethod
#     def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
#         pass

#     @abstractmethod
#     def next_month(self, current: ContractMonth) -> ContractMonth:
#         pass

#     @abstractmethod
#     def previous_month(self, current: ContractMonth) -> ContractMonth:
#         pass

# for range_ in self.ranges:
#     if current in range_:
#         return range_.cycle.previous_month(current=current)

# is_between = current not in self
# if is_between:
#     r = [
#         r for r in self.ranges
#         if r.end_month <= current
#     ][-1]
#     return r.end_month
# else:
#     r = [
#         r for r in self.ranges
#         if r.end_month is None or r.end_month <= current
#     ][-1]
#     print(r)
#     exit()
#     return r.end_month

# is_between = all(current not in r for r in self.ranges)
# print(is_between)

# # between ranges
# for i in range(0, len(self.ranges) - 1):
#     range1 = self.ranges[i]
#     range2 = self.ranges[i + 1]
#     is_between = (current >= range1.end_month) \
#                 and current < range2.start_month

#     if is_between:
#         return range1.end_month

# in ranges
