from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from nautilus_trader.common.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.price import ContinuousPrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from pyfutures.continuous.chain import ContractId

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: FuturesChain,
        start_month: ContractMonth,
        # end_month: ContractMonth | None,
        handler: Callable | None = None,
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
        self._last_received = False
        # self._end_month = end_month
        
    @property
    def roll_date_utc(self) -> pd.Timestamp:
        return self.current_id.roll_date_utc
    
    @property
    def approximate_expiry_date_utc(self) -> pd.Timestamp:
        return self.current_id.approximate_expiry_date_utc
    
    def on_start(self) -> None:
        # start = self._start_month.timestamp_utc
        """
        if the start month is in the hold cycle, do nothing
        if the start month is not in the hold cycle, go to next month in the hold cycle
        """
        
        start_month = self._start_month
        if self._start_month not in self._chain.hold_cycle:
            start_month = self._chain.hold_cycle.next_month(start_month)
            
        self.current_id = self._chain.make_id(start_month)
        self.roll()

    def on_bar(self, bar: Bar, is_last: bool = False) -> None:
            
        if bar.bar_type != self.current_bar_type and bar.bar_type != self.forward_bar_type:
            return
                
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        if current_bar is not None:
            self._send_continous_price()
            
        # next bar arrived before current or vice versa
        if current_bar is None or forward_bar is None:
            self.recv_count += 1
            return
            
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        current_day = current_timestamp.floor("D")
        forward_day = forward_timestamp.floor("D")
        
        # in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)
        in_roll_window = (current_timestamp >= self.roll_date) and (current_day <= self.expiry_day)
        
        # a valid roll time is where both timestamps are equal
        should_roll = in_roll_window and current_timestamp == forward_timestamp
        # should_roll = in_roll_window and current_day == forward_day
        
        # # for debugging
        # if self.current_id.month.value == "1995H":
        #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
        #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
        #     print(
        #         f"{self.current_id.month.value} {current_timestamp_str}",
        #         f"{self.forward_id.month.value} {forward_timestamp_str}",
        #         str(self.roll_date)[:-15],
        #         str(self.expiry_date)[:-15],
        #         should_roll,
        #     )
            
        if should_roll:
            self.unsubscribe_bars(self.current_bar_type)
            self.current_id = self._chain.forward_id(self.current_id)
            self.roll()
        else:
            is_expired = current_timestamp >= (self.expiry_day + pd.Timedelta(days=1))
            if is_expired:
                raise ValueError(f"ContractExpired {self.current_id}")
            elif is_last and bar.bar_type == self.current_bar_type:
                raise ValueError(f" contract failed to roll before last bar of current contract received {self.current_id}")
            
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

    def roll(self, message: str = "") -> None:
        
        assert self.current_id is not None

        self.forward_id = self._chain.forward_id(self.current_id)
        self.carry_id = self._chain.carry_id(self.current_id)
        
        self.current_bar_type = BarType(
            instrument_id=self.current_id.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

        self.forward_bar_type = BarType(
            instrument_id=self.forward_id.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

        self.carry_bar_type = BarType(
            instrument_id=self.carry_id.instrument_id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        self.subscribe_bars(self.carry_bar_type)
        
        self.expiry_date = self.current_id.approximate_expiry_date_utc
        self.expiry_day = self.expiry_date.floor("D")
        self.roll_date = self.current_id.roll_date_utc
        self._last_received = False
        print(f"{message}: {self.current_id}")
                
    
        


# print(f"expiry_date: {expiry_date}")

    # def _try_roll(self, bar: Bar, is_last: bool = False) -> None:
        # print("_try_roll")
        
        # if unix_nanos_to_dt(bar.ts_event) > pd.Timestamp("1987-10-20 14:32:00+00:00", tz="UTC"):
        #     exit()
            
        # if bar.bar_type.instrument_id.symbol.value.endswith("1987Z") \
        #     or bar.bar_type.instrument_id.symbol.value.endswith("1988H"):
        #     expiry_date = self.current_id.approximate_expiry_date_utc
        #     roll_date = self.current_id.roll_date_utc
        #     current_timestamp = unix_nanos_to_dt(bar.ts_event)
        #     in_window = (current_timestamp >= roll_date) and (current_timestamp < expiry_date)
            # if in_window:
        
                # roll_date = 1987-10-15 00:00:00+00:00
                # expiry_date = 1987-12-14 00:00:00+00:00
                
        
