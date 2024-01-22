from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from nautilus_trader.common.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import FuturesContractChain
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.price import ContinuousPrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: FuturesContractChain,
        start_month: ContractMonth,
        # end_month: ContractMonth | None,
        handler: Callable | None = None,
        raise_expired: bool = True,
        ignore_expiry_date: bool = False,
    ):
        super().__init__()
        
        self.current_bar_type = None  # initialized on start
        self.forward_bar_type = None  # initialized on start
        self.carry_bar_type = None  # initialized on start
        self.current_id = None  # initialized on start
        self.forward_id = None  # initialized on start
        self.carry_id = None  # initialized on start
        self.recv_count = 0
        
        self._bar_type = bar_type
        self._chain = chain
        self._instrument_id = bar_type.instrument_id
        self._handler = handler
        self._start_month = start_month
        self._raise_expired = raise_expired
        self._ignore_expiry_date = ignore_expiry_date
        
    def on_start(self) -> None:
        
        # if the start month is in the hold cycle, do nothing
        # if the start month is not in the hold cycle, go to next month in the hold cycle
        start_month = self._start_month
        if self._start_month not in self._chain.hold_cycle:
            start_month = self._chain.hold_cycle.next_month(start_month)
            
        self.current_id = self._chain.make_id(start_month)
        self.roll()

    def on_bar(self, bar: Bar) -> None:
        
        if self._raise_expired:
            is_expired = unix_nanos_to_dt(bar.ts_event) >= (self.expiry_day + pd.Timedelta(days=1))
            if is_expired:
                # TODO: wait for next forward bar != last timestamp
                raise ValueError(f"ContractExpired {self.current_id}")
            
        if bar.bar_type != self.current_bar_type and bar.bar_type != self.forward_bar_type:
            return
                
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        # # for debugging
        # if self.current_id.month.value == "1987V":
        #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
        #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
        #     print(
        #         f"{self.current_id.month.value} {current_timestamp_str}",
        #         f"{self.forward_id.month.value} {forward_timestamp_str}",
        #         str(self.roll_date)[:-15],
        #         str(self.expiry_date)[:-15],
        #         # should_roll,
        #         # current_timestamp >= self.roll_date,
        #         # current_day <= self.expiry_day,
        #     )
            
        if current_bar is not None:
            self._send_continous_price()
            
        # next bar arrived before current or vice versa
        if current_bar is None or forward_bar is None:
            self.recv_count += 1
            return
            
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        
        # in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)
        if self._ignore_expiry_date:
            in_roll_window = (current_timestamp >= self.roll_date)
        else:
            current_day = current_timestamp.floor("D")
            in_roll_window = (current_timestamp >= self.roll_date) and (current_day <= self.expiry_day)
            
        # a valid roll time is where both timestamps are equal
        should_roll = in_roll_window and current_timestamp == forward_timestamp
        if should_roll:
            self.roll()
            
        self.recv_count += 1
            
    def _send_continous_price(self) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        carry_bar = self.cache.bar(self.carry_bar_type)

        continuous_price = ContinuousPrice(
            instrument_id=self._instrument_id,
            current_price=Price(current_bar.close, current_bar.close.precision),
            current_month=self.current_id.month,
            forward_price=Price(forward_bar.close, forward_bar.close.precision)
            if forward_bar is not None
            else None,
            forward_month=self.forward_id.month,
            carry_price=Price(carry_bar.close, carry_bar.close.precision)
            if carry_bar is not None
            else None,
            carry_month=self.carry_id.month,
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )

        if self._handler is not None:
            self._handler(continuous_price)

        self._msgbus.publish(topic=f"{self._bar_type}0", msg=continuous_price)

    def roll(self) -> None:
        
        assert self.current_contract is not None
        
        self.current_contract = self._chain.forward_contract(self.current_contract)
        self.forward_contract = self._chain.forward_contract(self.current_contract)
        self.carry_contract = self._chain.carry_contract(self.current_contract)
        
        self.current_bar_type = BarType(
            instrument_id=self.current_contract.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
        # TODO: unsubscribe to old bars self.unsubscribe_bars(self.current_bar_type)
        
        self.forward_bar_type = BarType(
            instrument_id=self.forward_contract.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

        self.carry_bar_type = BarType(
            instrument_id=self.carry_contract.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        self.subscribe_bars(self.carry_bar_type)
        
        self.expiry_date = unix_nanos_to_dt(self.current_contract.expiration_ns)
        self.expiry_day = self.expiry_date.floor("D")
        self.roll_date = unix_nanos_to_dt(self.current_contract.roll_date_ns)
        