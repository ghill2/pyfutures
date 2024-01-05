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
        end_month: ContractMonth | None,
        handler: Callable | None = None,
    ):
        super().__init__()
        
        self.current_bar_type = None  # initialized on start
        self.forward_bar_type = None  # initialized on start
        self.carry_bar_type = None  # initialized on start
        self.current_id = None  # initialized on start
        self.forward_id = None  # initialized on start
        self.carry_id = None  # initialized on start
        
        self._bar_type = bar_type
        self._chain = chain
        self._instrument_id = bar_type.instrument_id
        self._handler = handler
        self._start_month = start_month
        self._end_month = end_month

    @property
    def roll_date_utc(self) -> pd.Timestamp:
        return self.current_id.roll_date_utc
    
    @property
    def approximate_expiry_date_utc(self) -> pd.Timestamp:
        return self.current_id.approximate_expiry_date_utc
    
    def on_start(self) -> None:
        start = self._start_month
        if isinstance(start, ContractMonth):
            start = start.timestamp_utc

        self.current_id = self._chain.current_id(start)
        self.roll()

    def on_bar(self, bar: Bar) -> None:
        
        # if self.current_id.month > self._end_month:
        #     return  # do nothing
        
        if bar.bar_type == self.current_bar_type or bar.bar_type == self.forward_bar_type:
            self._try_roll()

        if bar.bar_type == self.current_bar_type:
            self._send_continous_price()

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
        
        print(self.current_id)

    def _try_roll(self) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if current_bar is None or forward_bar is None:
            return  # next bar arrived before current or vice versa

        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)

        expiry_date = self.current_id.approximate_expiry_date_utc
        if current_timestamp >= expiry_date:
            # TODO: special handling
            raise ValueError("Contract has expired")

        roll_date = self.current_id.roll_date_utc
        in_window = (current_timestamp >= roll_date) and (current_timestamp < expiry_date)

        if not in_window:
            return

        if current_timestamp != forward_timestamp:
            return  # a valid roll time is where both timestamps are equal

        self.unsubscribe_bars(self.current_bar_type)
        self.current_id = self._chain.forward_id(self.current_id)
        self.roll()
