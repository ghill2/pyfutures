from __future__ import annotations
import pandas as pd

from pyfutures.continuous.contract_month import ContractMonth
from dataclasses import dataclass

class RollCycle:
    def __init__(self, value: str, skip_months: list[ContractMonth] | None = None):
        assert isinstance(value, str)

        self.value = "".join(sorted(value))
        
        self._skip_months = skip_months or []
    
    @classmethod
    def from_str(
        cls,
        value: str,
        skip_months: list[str] | None = None,
    ) -> RollCycle | RangedRollCycle:
        
        ranges = []
        if ">" in value:
            subs = value.replace("", "").split(",")
            for sub in subs:
                ranges.append(
                    RollCycleRange(
                        start_month=ContractMonth(sub.split(">")[0]),
                        end_month=ContractMonth(sub.split(">")[1].split("=")[0]),
                        cycle=RollCycle(sub.split(">")[1].split("=")[1]),
                    )
                )
            return RangedRollCycle(ranges=ranges)
        else:
            return RollCycle(value, skip_months=skip_months)
            
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
            raise NotImplementedError()  # TODO

@dataclass
class RollCycleRange:
    
    start_month: ContractMonth
    end_month: ContractMonth
    cycle: RollCycle
    
    def __contains__(self, month: ContractMonth) -> bool:
        return (month >= self.start_month) and (month <= self.end_month)
    
class RangedRollCycle:
    def __init__(self, ranges: list[RollCycleRange]):
        
        # TODO: check ranges are not overlapping
        # TODO: check no gap between ranges
        self._ranges = ranges
    
    def __contains__(self, month: ContractMonth) -> bool:
        for range in self._ranges:
            if month in range:
                return True
        return False
    
    def next_month(self, current: ContractMonth) -> ContractMonth:
        
        # between ranges
        for i in range(0, len(self._ranges) - 1):
            range1 = self._ranges[i]
            range2 = self._ranges[i + 1]
            is_between = current >= range1.end_month and current < range2.start_month
            if is_between:
                return range2.start_month
        
        # in ranges
        for range_ in self._ranges:
            if current in range_:
                return range_.cycle.next_month(current=current)
                
        raise RuntimeError()
                
    
        
        
            
        
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
        