from dataclasses import dataclass
from collections import deque

import pandas as pd
from nautilus_trader.common.actor import Actor
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import DataType
from nautilus_trader.data.messages import Subscribe
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading import Strategy
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.data import Bar
from nautilus_trader.model.orders.market import MarketOrder

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.multiple_bar import MultipleBar


@dataclass
class RollEvent:
    from_instrument_id: InstrumentId
    to_instrument_id: InstrumentId


class ContractChain(Actor):
    def __init__(
        self,
        config: ContractChainConfig,
        instrument_provider: InstrumentProvider,
    ):
        super().__init__()

        self.hold_cycle = config.roll_config.hold_cycle
        self.bar_type = config.bar_type
        self._adjusted: deque[float] = deque(maxlen=config.maxlen)
        self._current: deque[Bar] = deque(maxlen=config.maxlen)
        self.topic = f"{self.bar_type}r"
        self.rolls: pd.DataFrame = pd.DataFrame(columns=["timestamp", "to_month"])
        
        self._roll_offset = config.roll_config.roll_offset
        self._carry_offset = config.roll_config.carry_offset
        self._priced_cycle = config.roll_config.priced_cycle
        
        self._instrument_id = InstrumentId.from_str(str(config.roll_config.instrument_id))
        
        self._start_month = config.start_month

        self.instrument_provider = instrument_provider

        assert self._roll_offset <= 0
        assert self._carry_offset == 1 or self._carry_offset == -1
        assert self._start_month in self.hold_cycle
        
        self._raise_expired = config.raise_expired
        self._ignore_expiry_date = config.ignore_expiry_date
    
    @property
    def adjusted(self) -> deque[float]:
        return self._adjusted
    
    @property
    def current(self) -> deque[Bar]:
        return self._current
    
    def on_start(self) -> None:
        self._roll(to_month=self._start_month)
        
    def on_bar(self, bar: Bar) -> None:
        
        is_current = bar.bar_type == self.current_bar_type
        is_forward = bar.bar_type == self.forward_bar_type
        if is_current:
            self._adjusted.append(float(bar.close))
            self._current.append(bar)
            
        if is_current or is_forward:
            has_rolled: bool = self._attempt_roll()
            
    def _attempt_roll(self) -> bool:
        
        # roll when current_bar.timestamp == forward_bar.timestamp and inside roll window
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        # # for debugging
        # if "DAY" in str(self.bar_type) and self.current_month.value == "1995K":
        #     # self._log.debug(repr(bar))
        #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
        #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
        #     print(
        #         f"{self.current_month.value} {current_timestamp_str} "
        #         f"{self.forward_month.value} {forward_timestamp_str} "
        #         f"{str(self.roll_date)[:-15]} "
        #         f"{str(self.expiry_date)[:-15]} "
        #     )
        
        # calculate the strategy on every current_bar
        
        
        # strategy calculates, create new position size, submit new position size to forward contract
        # needs to use forward price
        # add liquid window for execution
        if current_bar is None or forward_bar is None:
            return False
        
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        
        if current_timestamp != forward_timestamp:
            return False
        
        # check expiry date
        if not self._ignore_expiry_date:
            is_expired = current_timestamp >= (self.expiry_day + pd.Timedelta(days=1))
            if is_expired:
                raise ValueError("ContractExpired")
        
        if self._ignore_expiry_date:
            in_roll_window = current_timestamp >= self.roll_date
        else:
            current_day = current_timestamp.floor("D")
            in_roll_window = (current_timestamp >= self.roll_date) and (current_day <= self.expiry_day)
            
        if in_roll_window:
            self.roll()
            self.rolls.loc[len(self.rolls)] = (current_timestamp, self.current_month)
            return True
        
        return False
            
    def roll(self):
        to_month = self.hold_cycle.next_month(self.current_month)
        self._roll(to_month=to_month)
        
    def _roll(self, to_month: ContractMonth) -> None:
        self._log.debug(f"Rolling to month {to_month}...")
        if len(self._adjusted) > 0:
            self._adjust_values()
        self._update_attributes(to_month=to_month)
        self._update_subscriptions()
        # self._update_position()
        self._log.debug(
            f"Rolled {self.previous_contract.id} > {self.current_contract.id}"
        )
        
        self.msgbus.publish(
            topic=self.topic,
            msg=RollEvent(
                from_instrument_id=self.previous_contract.id,
                to_instrument_id=self.current_contract.id,
            ),
        )
        
    def _adjust_values(self) -> None:
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        adjustment_value = float(forward_bar.close) - float(current_bar.close)
        
        self._adjusted = deque(
            [x + adjustment_value for x in self._adjusted],
            maxlen=self._adjusted.maxlen,
        )
        
    def _fetch_contract(self, month: ContractMonth) -> FuturesContract:
        if self.instrument_provider.get_contract(self._instrument_id, month) is None:
            self.instrument_provider.load_contract(instrument_id=self._instrument_id, month=month)
        return self.instrument_provider.get_contract(self._instrument_id, month)
    
    def _update_subscriptions(self) -> None:
        """
        Updating subscriptions after the roll
        """
        self._log.debug("Updating subscriptions...")
        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
    
    def _update_attributes(self, to_month: ContractMonth) -> None:
        
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
        self.expiry_day: pd.Timestamp = self.expiry_date.floor("D")

        # TODO: factor in the trading calendar
        self.roll_date: pd.Timestamp = self.expiry_date + pd.Timedelta(days=self._roll_offset)
        
        self.current_bar_type = BarType(
            instrument_id=self.current_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.previous_bar_type = BarType(
            instrument_id=self.previous_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.forward_bar_type = BarType(
            instrument_id=self.forward_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.carry_bar_type = BarType(
            instrument_id=self.carry_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
        
        if self.cache.instrument(self.current_contract.id) is None:
            self.cache.add_instrument(self.current_contract)

        if self.cache.instrument(self.forward_contract.id) is None:
            self.cache.add_instrument(self.forward_contract)

        if self.cache.instrument(self.carry_contract.id) is None:
            self.cache.add_instrument(self.carry_contract)
    
    
    
                
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


        