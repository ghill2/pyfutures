from __future__ import annotations

from pyfutures.continuous.contract_month import ContractMonth


class RollCycle:
    def __init__(
        self,
        value: str,
        skip_months: list[ContractMonth] | None = None,
    ):
        assert isinstance(value, str)

        self.value = "".join(sorted(value))

        self._skip_months = skip_months or []

    def next_month(self, current: ContractMonth) -> ContractMonth:
        """
        Return the next month in the cycle.

        Returns
        -------
        ContractMonth

        """
        year = current.year
        letter_month = current.letter_month

        if letter_month not in self.value:
            return self._closest_next(current)

        is_last_month = self.value[-1] == letter_month
        if is_last_month:
            year += 1
            letter_month = self.value[0]
        else:
            letter_month = self.value[self.value.index(letter_month) + 1]

        month = ContractMonth(f"{year}{letter_month}")

        while month in self._skip_months:
            month = self.next_month(current=month)

        return month

    def previous_month(self, current: ContractMonth) -> ContractMonth:
        """
        Return the previous month in the cycle.

        Returns
        -------
        ContractMonth

        """
        year = current.year
        letter_month = current.letter_month

        if letter_month not in self.value:
            return self._closest_previous(current)

        is_first_month = self.value[0] == letter_month
        if is_first_month:
            year -= 1
            letter_month = self.value[-1]
        else:
            letter_month = self.value[self.value.index(letter_month) - 1]

        month = ContractMonth(f"{year}{letter_month}")

        while month in self._skip_months:
            month = self.next_month(current=month)

        return month

    def _closest_previous(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

        for char in self.value[::-1]:
            if char < letter_month:
                return ContractMonth(f"{year}{char}")

        year -= 1
        return ContractMonth(f"{year}{self.value[-1]}")

    def _closest_next(self, current: ContractMonth) -> ContractMonth:
        year = current.year
        letter_month = current.letter_month

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

    def get_months(self, start: ContractMonth, end: ContractMonth) -> set[ContractMonth]:
        months = set()
        assert start in self
        while start < end:
            months.add(start)
            start = self.next_month(start)
        return sorted(months)

    def __getstate__(self):
        return (self.value, self._skip_months)

    def __setstate__(self, state):
        self.value = state[0]
        self._skip_months = state[1]
