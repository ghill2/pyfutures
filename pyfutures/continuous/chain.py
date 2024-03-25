import pandas as pd
from nautilus_trader.common.component import Clock
from nautilus_trader.common.component import Logger
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth


class ContractChain:
    def __init__(
        self,
        config: ContractChainConfig,
        clock: Clock,
    ):
        self.instrument_id = config.instrument_id
        self.rolls = pd.DataFrame(columns=["timestamp", "to_month"])
        self.current_month: ContractMonth | None = None
        self.previous_month: ContractMonth | None = None
        self.forward_month: ContractMonth | None = None
        self.carry_month: ContractMonth | None = None
        self.current_contract_id: InstrumentId | None = None
        self.forward_contract_id: InstrumentId | None = None
        self.carry_contract_id: InstrumentId | None = None
        self.previous_contract_id: InstrumentId | None = None
        self.expiry_date: pd.Timestamp | None = None
        self.roll_date: pd.Timestamp | None = None
        self.roll_offset = config.roll_config.roll_offset
        self.carry_offset = config.roll_config.carry_offset
        self.priced_cycle = config.roll_config.priced_cycle
        self.hold_cycle = config.roll_config.hold_cycle
        self.approximate_expiry_offset = config.roll_config.approximate_expiry_offset
        self.start_month = config.start_month
        self.config = config

        assert self.roll_offset <= 0
        assert self.carry_offset == 1 or self.carry_offset == -1
        assert self.start_month in self.hold_cycle

        self._log = Logger(name=type(self).__name__)
        self._clock = clock

    def start(self) -> None:
        self.roll(to_month=self.start_month)

    def roll(
        self,
        to_month: ContractMonth | None = None,
    ) -> None:
        """
        Roll to specified `ContractMonth` in the chain.
        Rolls to the next month in the hold cycle if no `ContractMonth` is passed.
        """
        to_month = to_month or self.hold_cycle.next_month(self.current_month)

        self.current_month = to_month
        self.previous_month = self.hold_cycle.previous_month(self.current_month)
        self.forward_month = self.hold_cycle.next_month(self.current_month)
        if self.carry_offset == 1:
            self.carry_month = self.priced_cycle.next_month(self.current_month)
        elif self.carry_offset == -1:
            self.carry_month = self.priced_cycle.previous_month(self.current_month)

        self.current_contract_id = self.format_instrument_id(self.current_month)
        self.forward_contract_id = self.format_instrument_id(self.forward_month)
        self.carry_contract_id = self.format_instrument_id(self.carry_month)
        self.previous_contract_id = self.format_instrument_id(self.previous_month)

        self.roll_date, self.expiry_date = self.roll_window(self.current_month)

        self._log.debug(
            f"Rolled {self.previous_contract_id} > {self.current_contract_id}",
        )

        self.rolls.loc[len(self.rolls)] = (self._clock.utc_now(), self.current_month)

    def roll_window(
        self,
        month: ContractMonth,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        # TODO: for live environment the expiry date from the contract should be used
        expiry_date = month.timestamp_utc + pd.Timedelta(days=self.approximate_expiry_offset)
        roll_date = expiry_date + pd.Timedelta(days=self.roll_offset)
        return (roll_date, expiry_date)

    def format_instrument_id(self, month: ContractMonth) -> InstrumentId:
        """
        Format the InstrumentId for contract given the ContractMonth.
        """
        symbol = self.instrument_id.symbol.value
        venue = self.instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}={month.year}{month.letter_month}.{venue}",
        )
