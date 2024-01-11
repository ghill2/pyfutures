from dataclasses import dataclass

import pandas as pd

from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.identifiers import InstrumentId


@dataclass
class ContractId:
    instrument_id: InstrumentId
    month: ContractMonth
    approximate_expiry_date_utc: pd.Timestamp
    roll_date_utc: pd.Timestamp


class FuturesChain:
    def __init__(
        self,
        config: FuturesChainConfig,
    ):
        self.instrument_id = InstrumentId.from_str(str(config.instrument_id))
        self.hold_cycle = RollCycle(config.hold_cycle)
        self.priced_cycle = RollCycle(config.priced_cycle)
        self.roll_offset = config.roll_offset
        self.approximate_expiry_offset = config.approximate_expiry_offset
        self.carry_offset = config.carry_offset

        assert self.roll_offset <= 0
        assert self.carry_offset == 1 or self.carry_offset == -1

    def approximate_expiry_date(self, month: ContractMonth) -> pd.Timestamp:
        """
        Return the approximate expiry date of the month.
        """
        return month.timestamp_utc + pd.Timedelta(days=self.approximate_expiry_offset)

    def roll_date(self, month: ContractMonth) -> pd.Timestamp:
        """
        Return the date the roll should occur at the month.
        """
        return self.approximate_expiry_date(month) + pd.Timedelta(days=self.roll_offset)

    def carry_id(self, current: ContractId) -> ContractId:
        return self.make_id(self.carry_month(current.month))

    def forward_id(self, current: ContractId) -> ContractId:
        return self.make_id(self.forward_month(current.month))

    def current_id(self, timestamp: pd.Timestamp) -> ContractId:
        return self.make_id(self.current_month(timestamp))

    def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
        current = self.hold_cycle.current_month(timestamp)

        while True:
            roll_date = self.roll_date(current)

            if roll_date > timestamp:
                break

            current = self.hold_cycle.next_month(current)

        return current

    def forward_month(self, month: ContractMonth) -> ContractMonth:
        return self.hold_cycle.next_month(month)

    def carry_month(self, month: ContractMonth) -> ContractMonth:
        if self.carry_offset == 1:
            return self.priced_cycle.next_month(month)
        elif self.carry_offset == -1:
            return self.priced_cycle.previous_month(month)
        else:
            raise ValueError("carry offset must be 1 or -1")

    def make_id(self, month: ContractMonth) -> ContractId:
        return ContractId(
            instrument_id=self._fmt_instrument_id(self.instrument_id, month),
            month=month,
            approximate_expiry_date_utc=self.approximate_expiry_date(month),
            roll_date_utc=self.roll_date(month),
        )

    @staticmethod
    def _fmt_instrument_id(base: InstrumentId, month: ContractMonth) -> InstrumentId:
        """
        Return the InstrumentId of a specific month.
        """
        return InstrumentId.from_str(
            f"{base.symbol.value}={month}.{base.venue.value}",
        )
