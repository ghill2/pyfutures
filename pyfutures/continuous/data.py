from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from collections import deque
from pyfutures.continuous.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from datetime import timedelta

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
        handler: Callable | None = None,
        lookback: int | None = 10_000,
    ):
        super().__init__()
        
        self._bar_type = bar_type
        self._chain = chain
        self.recv_count = 0
        self.prices = deque(maxlen=lookback)
        self._handler = handler
    
    @property
    def current_bar_type(self):
        return BarType(
            instrument_id=self._chain.current_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
        
    @property
    def forward_bar_type(self):
        return BarType(
            instrument_id=self._chain.forward_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
        
    @property
    def carry_bar_type(self):
        return BarType(
            instrument_id=self._chain.carry_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
        
    @property
    def current_bar(self):
        return self.cache.bar(self.current_bar_type)
    
    @property
    def forward_bar(self):
        return self.cache.bar(self.forward_bar_type)
    
    @property
    def carry_bar(self):
        return self.cache.bar(self.carry_bar_type)
    
    def on_bar(self, bar: Bar) -> None:
        
        
        is_forward_or_current = \
            (bar.bar_type == self.current_bar_type or bar.bar_type == self.forward_bar_type)
            
        if not is_forward_or_current:
            return
            
        current_timestamp = unix_nanos_to_dt(self.current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(self.forward_bar.ts_event)
        
        if current_timestamp == forward_timestamp:
            self.send_multiple_price()
        
    def _send_multiple_price(self):
        
        carry_bar = self.carry_bar
        forward_bar = self.forward_bar
        current_bar = self.current_bar
        
        multiple_price = MultiplePrice(
            bar_type=self.bar_type,
            current_price=Price(current_bar.close, current_bar.close.precision),
            current_month=self._chain.current_month,
            forward_price=Price(forward_bar.close, forward_bar.close.precision)
            if forward_bar is not None
            else None,
            forward_month=self._chain.forward_month,
            carry_price=Price(carry_bar.close, carry_bar.close.precision)
            if carry_bar is not None
            else None,
            carry_month=self._chain.carry_month,
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )

        if self._handler is not None:
            self._handler(multiple_price)
            
        self.prices.append(multiple_price)

        self._msgbus.publish(topic=f"{self.bar_type}0", msg=multiple_price)
        
        
    