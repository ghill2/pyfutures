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
        return self.value == other.value

    def __str__(self) -> str:
        return self.value

    def __contains__(self, month: ContractMonth) -> bool:
        return month.letter_month in self.value

class CycleIterator:
    def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
        raise NotImplementedError()
    
    def next_month(self, current: ContractMonth) -> ContractMonth:
        raise NotImplementedError()
    
    def previous_month(self, current: ContractMonth) -> ContractMonth:
        raise NotImplementedError()
    
class RollCycleRange:
    def __init__(
        self,
        start_month: ContractMonth,
        end_month: ContractMonth,
        cycle: RollCycle,
    ):
        self.start_month = start_month
        self.end_month = end_month
        self.cycle = cycle
        
class RangedRollCycle:
    def __init__(self, ranges: list[RollCycleRange]):
        pass