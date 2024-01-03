import pandas as pd

from nautilus_trader.model.continuous.contract_month import ContractMonth


class RollCycle:
    def __init__(self, value: str):
        assert isinstance(value, str)

        self.value = "".join(sorted(value))

    def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
        current_id = ContractMonth.from_month_year(timestamp.year, timestamp.month)

        letter_month = current_id.letter_month

        if letter_month not in self.value:
            current_id = self.next_month(current_id)

        return current_id

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

        year_str = str(year).zfill(2)
        return ContractMonth(f"{month}{year_str[-2]}{year_str[-1]}")

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

        year_str = str(year).zfill(2)
        return ContractMonth(f"{month}{year_str[-2]}{year_str[-1]}")

    def _closest_previous(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        assert letter_month not in self.value
        for char in self.value[::-1]:
            if char < letter_month:
                year_str = str(year)
                return ContractMonth(f"{char}{year_str[-2]}{year_str[-1]}")

        year -= 1
        year_str = str(year)
        return ContractMonth(f"{self.value[-1]}{year_str[-2]}{year_str[-1]}")

    def _closest_next(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        assert letter_month not in self.value

        for char in self.value:
            if char > letter_month:
                year_str = str(year)
                return ContractMonth(f"{char}{year_str[-2]}{year_str[-1]}")

        year += 1
        year_str = str(year)
        return ContractMonth(f"{self.value[0]}{year_str[-2]}{year_str[-1]}")

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
