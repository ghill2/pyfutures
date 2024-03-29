from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.contract_month import ContractMonth


class ContractChain:
    def __init__(
        self,
        bar_type: BarType,
        config: RollConfig,
    ):
        self.roll_offset = config.roll_offset
        self.carry_offset = config.carry_offset
        self.priced_cycle = config.priced_cycle
        self.hold_cycle = config.hold_cycle

        # assert self._start_month is not None
        # assert self._start_month in self.hold_cycle

    def set_month(self, month: ContractMonth) -> None:
        pass

    @property
    def get_current_month(self) -> ContractMonth:
        # return the current position based on the timestamp
        pass

    @property
    def forward_month(self) -> ContractMonth:
        return self.hold_cycle.next_month(self.current_month)

    @property
    def carry_month(self) -> ContractMonth:
        if self.carry_offset == 1:
            return self.priced_cycle.next_month(self.current_month)
        elif self.carry_offset == -1:
            return self.priced_cycle.previous_month(self.current_month)

    @property
    def current_bar_type(self) -> BarType:
        return self._make_bar_type(self.current_month)

    @property
    def previous_month(self) -> ContractMonth:
        return self.hold_cycle.previous_month(self.current_month)

    @property
    def previous_bar_type(self) -> BarType:
        return self._make_bar_type(self.previous_month)

    @property
    def forward_bar_type(self) -> BarType:
        return self._make_bar_type(self.forward_month)

    @property
    def carry_bar_type(self) -> BarType:
        return self._make_bar_type(self.carry_month)

    def _make_bar_type(self, month: ContractMonth) -> BarType:
        return BarType(
            instrument_id=self.format_instrument_id(month),
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

    def format_instrument_id(self, month: ContractMonth) -> InstrumentId:
        """
        Format the InstrumentId for contract given the ContractMonth.
        """
        symbol = self._instrument_id.symbol.value
        venue = self._instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}={month.year}{month.letter_month}.{venue}",
        )


# class ContractExpired(Exception):
#     pass


# class ContractChain(Actor):
#     def __init__(
#         self,
#         config: ContractChainConfig,
#     ):
#         super().__init__()
#         self.instrument_id = config.instrument_id
#         self.rolls = pd.DataFrame(columns=["timestamp", "to_month"])
#         self.current_month: ContractMonth | None = None
#         self.previous_month: ContractMonth | None = None
#         self.forward_month: ContractMonth | None = None
#         self.carry_month: ContractMonth | None = None
#         self.current_contract_id: InstrumentId | None = None
#         self.forward_contract_id: InstrumentId | None = None
#         self.carry_contract_id: InstrumentId | None = None
#         self.previous_contract_id: InstrumentId | None = None
#         self.expiry_date: pd.Timestamp | None = None
#         self.roll_date: pd.Timestamp | None = None
#         self.roll_offset = config.roll_config.roll_offset
#         self.carry_offset = config.roll_config.carry_offset
#         self.priced_cycle = config.roll_config.priced_cycle
#         self.hold_cycle = config.roll_config.hold_cycle
#         self.approximate_expiry_offset = config.roll_config.approximate_expiry_offset
#         self.start_month = config.start_month

#         assert self.roll_offset <= 0
#         assert self.carry_offset == 1 or self.carry_offset == -1
#         assert self.start_month in self.hold_cycle

#     def on_start(self) -> None:
#         self.roll(to_month=self.start_month)

#     def attempt_roll(self, bar: ContinuousBar):
#         should_roll: bool = self._should_roll(bar)
#         if should_roll:
#             self.roll()

#     def _should_roll(self, bar: ContinuousBar) -> bool:
#         if bar.forward_bar is None:
#             return False

#         forward_timestamp = unix_nanos_to_dt(bar.forward_bar.ts_init)
#         current_timestamp = unix_nanos_to_dt(bar.current_bar.ts_init)

#         if current_timestamp != forward_timestamp:
#             return False

#         in_roll_window = (current_timestamp >= self.chain.roll_date) and (current_timestamp < self.chain.expiry_date)

#         return in_roll_window

#     def roll(
#         self,
#         to_month: ContractMonth | None = None,
#     ) -> None:
#         """
#         Roll to specified `ContractMonth` in the chain.
#         Rolls to the next month in the hold cycle if no `ContractMonth` is passed.
#         """
#         to_month = to_month or self.hold_cycle.next_month(self.current_month)

#         self.current_month = to_month
#         self.previous_month = self.hold_cycle.previous_month(self.current_month)
#         self.forward_month = self.hold_cycle.next_month(self.current_month)
#         if self.carry_offset == 1:
#             self.carry_month = self.priced_cycle.next_month(self.current_month)
#         elif self.carry_offset == -1:
#             self.carry_month = self.priced_cycle.previous_month(self.current_month)

#         self.current_contract_id = self.format_instrument_id(self.current_month)
#         self.forward_contract_id = self.format_instrument_id(self.forward_month)
#         self.carry_contract_id = self.format_instrument_id(self.carry_month)
#         self.previous_contract_id = self.format_instrument_id(self.previous_month)

#         # self.roll_date, self.expiry_date = self.roll_window(self.current_month)

#         self._log.debug(
#             f"Rolled {self.previous_contract_id} > {self.current_contract_id}",
#         )

#         now = self._clock.timestamp_ns()
#         self.rolls.loc[len(self.rolls)] = (now, self.current_month)

#         event = RollEvent(
#             ts_init=self._clock.timestamp_ns(),
#             from_instrument_id=self.previous_contract_id,
#             to_instrument_id=self.current_contract_id,
#         )

#         self.msgbus.publish(
#             topic=f"events.roll.{self.instrument_id}",
#             msg=event,
#         )

#     def roll_window(
#         self,
#         month: ContractMonth,
#     ) -> tuple[pd.Timestamp, pd.Timestamp]:
#         # TODO: for live environment the expiry date from the contract should be used
#         expiry_date = month.timestamp_utc + pd.Timedelta(days=self.approximate_expiry_offset)
#         roll_date = expiry_date + pd.Timedelta(days=self.roll_offset)
#         return (roll_date, expiry_date)

#     def format_instrument_id(self, month: ContractMonth) -> InstrumentId:
#         """
#         Format the InstrumentId for contract given the ContractMonth.
#         """
#         symbol = self.instrument_id.symbol.value
#         venue = self.instrument_id.venue.value
#         return InstrumentId.from_str(
#             f"{symbol}={month.year}{month.letter_month}.{venue}",
#         )
