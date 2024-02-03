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
from pyfutures.continuous.signal import RollSignal

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        signal: RollSignal,
        chain: ContractChain,
        handler: Callable | None = None,
        lookback: int | None = 10_000,
    ):
        super().__init__()
        
        self.prices = deque(maxlen=lookback)
        
        self._bar_type = bar_type
        self._chain = chain
        self._handler = handler
        self._signal = signal
    
    @property
    def instrument_id(self):
        return self.bar_type.instrument_id
    
    @property
    def bar_type(self):
        return self._bar_type
    
    @property
    def current_bar_type(self) -> BarType:
        return self._chain.current_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
    @property
    def forward_bar_type(self) -> BarType:
        return self._chain.forward_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
    @property
    def carry_bar_type(self) -> BarType:
        return self._chain.carry_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
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
    
    def on_start(self) -> None:
        
        """
        subscribe to roll events from the RollSignal actor for handling subscriptions after roll
        
        """
        self.msgbus.subscribe(
            topic=f"{self._signal.topic}r",
            handler=self._manage_subscriptions,
        )
        self._manage_subscriptions()
        
    def on_bar(self, bar: Bar) -> None:
        
        is_forward_or_current = \
            (bar.bar_type == self.current_bar_type or bar.bar_type == self.forward_bar_type)
            
        if not is_forward_or_current:
            return
        
        current_bar = self.current_bar
        forward_bar = self.forward_bar
        
        if current_bar is None or forward_bar is None:
            return
        
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        
        if current_timestamp != forward_timestamp:
            return
    
        self._send_multiple_price()
        
    def _send_multiple_price(self) -> None:
        
        multiple_price: MultiplePrice = self._create_multiple_price()
        
        if self._handler is not None:
            self._handler(multiple_price)
            
        self.prices.append(multiple_price)

        self._msgbus.publish(topic=f"{self.bar_type}0", msg=multiple_price)
             
    def _create_multiple_price(self) -> MultiplePrice:
        
        carry_bar = self.carry_bar
        forward_bar = self.forward_bar
        current_bar = self.current_bar
        
        return MultiplePrice(
            bar_type=self.bar_type,
            current_price=Price(current_bar.close, current_bar.close.precision),
            current_bar_type=self.current_bar_type,
            current_month=self._chain.current_month,
            forward_price=Price(forward_bar.close, forward_bar.close.precision)
            if forward_bar is not None
            else None,
            forward_bar_type=self.forward_bar_type,
            forward_month=self._chain.forward_month,
            carry_price=Price(carry_bar.close, carry_bar.close.precision)
            if carry_bar is not None
            else None,
            carry_month=self._chain.carry_month,
            carry_bar_type=self.carry_bar_type,
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )
        
    def _manage_subscriptions(self) -> None:
        
        current_contract = self._chain.current_contract
        forward_contract = self._chain.forward_contract
        
        if self.cache.instrument(current_contract.id) is None:
            self.cache.add_instrument(self._chain.current_contract)
        
        if self.cache.instrument(forward_contract.id) is None:
            self.cache.add_instrument(forward_contract)
        
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        
    # def on_roll_event(self, event: RollEvent) -> None:
    #     """
    #     on startup, the data module gets the current, forward and carry bar type
    #     """
    #     pass