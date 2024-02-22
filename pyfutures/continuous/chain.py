from dataclasses import dataclass
import pandas as pd
from datetime import datetime

from nautilus_trader.core.uuid import UUID4
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType
from pyfutures.continuous.multiple_bar import MultipleBar
from nautilus_trader.common.actor import Actor


@dataclass
class RollEvent:
    bar_type: BarType


class ContractChain(Actor):
    def __init__(
        self,
        bar_type: BarType,
        config: ContractChainConfig,
        instrument_provider: InstrumentProvider,
        raise_expired: bool = True,
        ignore_expiry_date: bool = False,
    ):
        super().__init__()

        self.hold_cycle = config.hold_cycle
        self.bar_type = bar_type
        self.topic = f"{self.bar_type}r"

        self._roll_offset = config.roll_offset
        self._instrument_id = InstrumentId.from_str(str(config.instrument_id))
        self._carry_offset = config.carry_offset
        self._priced_cycle = config.priced_cycle
        self._start_month = config.start_month

        self._instrument_provider = instrument_provider

        assert self._roll_offset <= 0
        assert self._carry_offset == 1 or self._carry_offset == -1

        self._raise_expired = raise_expired
        self._ignore_expiry_date = ignore_expiry_date
        self._rolls: tuple[pd.Timestamp, ContractMonth] = []

    @property
    def rolls(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": [x[0] for x in self._rolls],
                "month": [x[1] for x in self._rolls],
            }
        )

    @property
    def instrument_provider(self) -> InstrumentProvider:
        return self._instrument_provider

    def on_start(self) -> None:
        month = self._start_month
        if month not in self.hold_cycle:
            month = self.hold_cycle.next_month(month)

        self._roll(to_month=month)

        self.msgbus.subscribe(
            topic=f"{self.bar_type}0",
            handler=self.on_multiple_price,
            # TODO determine priority
        )

    def on_multiple_price(self, price: MultipleBar) -> None:
        expiry_day = self.expiry_date.floor("D")
        roll_date = self.roll_date
        current_timestamp = unix_nanos_to_dt(price.ts_event)

        if not self._ignore_expiry_date:
            is_expired = current_timestamp >= (expiry_day + pd.Timedelta(days=1))
            if is_expired and self._raise_expired:
                raise ValueError(f"ContractExpired {self.bar_type}")

        if self._ignore_expiry_date:
            in_roll_window = current_timestamp >= roll_date
        else:
            current_day = current_timestamp.floor("D")
            in_roll_window = (current_timestamp >= roll_date) and (current_day <= expiry_day)

        if in_roll_window:
            to_month = self.hold_cycle.next_month(self.current_month)
            self._roll(to_month=to_month)
            self._rolls.append((current_timestamp, to_month))

    def roll(self):
        to_month = self.hold_cycle.next_month(self.current_month)
        self._roll(to_month=to_month)

    def _roll(
        self,
        to_month: ContractMonth,
    ) -> None:
        self._log.debug(f"Rolling to month {to_month}...")

        self.current_month: ContractMonth = to_month
        self.previous_month: ContractMonth = self.hold_cycle.previous_month(self.current_month)
        self.forward_month: ContractMonth = self.hold_cycle.next_month(self.current_month)

        # update carry month
        if self._carry_offset == 1:
            self.carry_month: ContractMonth = self._priced_cycle.next_month(self.current_month)
        elif self._carry_offset == -1:
            self.carry_month: ContractMonth = self._priced_cycle.previous_month(self.current_month)

        self.current_contract: FuturesContract = self._fetch_contract(self.current_month)
        self.previous_contract: FuturesContract = self._fetch_contract(self.previous_month)
        self.forward_contract: FuturesContract = self._fetch_contract(self.forward_month)
        self.carry_contract: FuturesContract = self._fetch_contract(self.carry_month)

        self.expiry_date: pd.Timestamp = unix_nanos_to_dt(self.current_contract.expiration_ns)

        # TODO: factor in the trading calendar
        self.roll_date: pd.Timestamp = self.expiry_date + pd.Timedelta(days=self._roll_offset)

        self.msgbus.publish(
            topic="events.roll",
            msg=RollEvent(bar_type=self.bar_type),
            # TODO determine priority
        )

    def _fetch_contract(self, month: ContractMonth) -> FuturesContract:
        if self._instrument_provider.get_contract(self._instrument_id, month) is None:
            self._instrument_provider.load_contract(instrument_id=self._instrument_id, month=month)
        return self._instrument_provider.get_contract(self._instrument_id, month)

    # self.current_instrument_id = self._fmt_instrument_id(self._instrument_id, self.current_month)
    # self.forward_instrument_id = self._fmt_instrument_id(self._instrument_id, self.forward_month)
    # self.carry_instrument_id = self._fmt_instrument_id(self._instrument_id, self.carry_month)

    # @property
    # def current_details(self) -> ChainDetails:
    #     return self._current_details

    # def _create_details(self, current: FuturesContract) -> None:

    #     self._current_details = ChainDetails(
    #         instrument_id=self._instrument_id,
    #         current_contract=current,
    #         forward_contract=self.forward_contract(current),
    #         carry_contract=self.carry_contract(current),
    #         expiry_date=unix_nanos_to_dt(current.expiration_ns),
    #         roll_date=self.get_roll_date(current),
    #     )

    # def current_month(self, month: ContractMonth) -> ContractMonth:
    #     if month in self.hold_cycle:
    #         return month

    #     return self.forward_month(month)

    # def forward_month(self, month: ContractMonth) -> ContractMonth:
    #     return self.hold_cycle.next_month(month)

    # def carry_month(self, month: ContractMonth) -> ContractMonth:

    # def current_month_from_timestamp(self, timestamp: pd.Timestamp) -> ContractMonth:

    #     current = self.hold_cycle.current_month(timestamp)

    #     while True:
    #         roll_date = self._roll_date_utc(
    #                         expiry_date=current.timestamp_utc,
    #                         offset=self._roll_offset,
    #                     )

    #         if roll_date > timestamp:
    #             break

    #         current = self.hold_cycle.next_month(current)

    #     return current

    # def make_id(self, month: ContractMonth) -> ContractId:
    #     return ContractId(
    #         instrument_id=self._fmt_instrument_id(self.instrument_id, month),
    #         month=month,
    #     )

    # def carry_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.carry_month(current.month))

    # def forward_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.forward_month(current.month))

    # def current_id(self, timestamp: pd.Timestamp) -> ContractId:
    #     return self.make_id(self.current_month(timestamp))

    # def carry_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.carry_month(current.month))

    # def forward_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.forward_month(current.month))

    # def current_id(self, timestamp: pd.Timestamp) -> ContractId:
    #     return self.make_id(self.current_month(timestamp))

    # f"{base.symbol.value}={month}.{base.venue.value}",

    # def make_id(self, month: ContractMonth) -> ContractId:
    #     return ContractId(
    #         instrument_id=self._fmt_instrument_id(self.instrument_id, month),
    #         month=month,
    #         approximate_expiry_date_utc=self.approximate_expiry_date(month),
    #         roll_date_utc=self.roll_date(month),
    #     )
    # @dataclass


# class ContractId:
#     instrument_id: InstrumentId
#     month: ContractMonth
#     approximate_expiry_date_utc: pd.Timestamp
#     roll_date_utc: pd.Timestamp

# def current_contract(self, month: ContractMonth) -> FuturesContract:
#     month = self.current_month(month)
#     self._instrument_provider.load_contract(instrument_id=self._instrument_id, month=month)
#     return self._instrument_provider.get_contract(self._instrument_id, month)

# def forward_contract(self, current: FuturesContract) -> FuturesContract:
#     month = self.forward_month(current.info["month"])
#     self._instrument_provider.load_contract(instrument_id=self._instrument_id, month=month)
#     return self._instrument_provider.get_contract(self._instrument_id, month)

# def carry_contract(self, current: FuturesContract) -> FuturesContract:
#     month = self.carry_month(current.info["month"])
#     self._instrument_provider.load_contract(instrument_id=self._instrument_id, month=month)
#     return self._instrument_provider.get_contract(self._instrument_id, month)

# def current_month(self, month: ContractMonth) -> ContractMonth:
#     if month in self.hold_cycle:
#         return month

#     return self.forward_month(month)

# def forward_month(self, month: ContractMonth) -> ContractMonth:
#     return self.hold_cycle.next_month(month)

# def carry_month(self, month: ContractMonth) -> ContractMonth:
#     if self._carry_offset == 1:
#         return self._priced_cycle.next_month(month)
#     elif self._carry_offset == -1:
#         return self._priced_cycle.previous_month(month)
#     else:
#         raise ValueError("carry offset must be 1 or -1")
# @dataclass
# class ChainDetails:
#     instrument_id: InstrumentId
#     current_contract: FuturesContract
#     forward_contract: FuturesContract
#     carry_contract: FuturesContract
#     expiry_date: pd.Timestamp
#     roll_date: pd.Timestamp
