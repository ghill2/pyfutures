from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from pyfutures.continuous.chain import ContractChain
from pyfutures.pyfutures.continuous.multiple_price import MultiplePrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from pyfutures.continuous.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt

class MultipleData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
    ):
        super().__init__()
        
        self.bar_type = bar_type
        self.topic = f"{self.bar_type}0"
        self.chain = chain
    
    def on_start(self) -> None:
        self._manage_subscriptions()
        
    def on_bar(self, bar: Bar) -> None:
        
        is_forward_or_current = \
            (bar.bar_type == self.current_bar_type or bar.bar_type == self.forward_bar_type)

        if not is_forward_or_current:
            return
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        # # for debugging
        # if "MINUTE" in str(self.bar_type) and self._chain.current_month.value == "1998M":
        #     # self._log.debug(repr(bar))
        #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
        #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
        #     self._log.debug(
        #         f"{self._chain.current_month.value} {current_timestamp_str} "
        #         f"{self._chain.forward_month.value} {forward_timestamp_str} "
        #         f"{str(self._chain.roll_date)[:-15]} "
        #         f"{str(self._chain.expiry_date)[:-15]} "
        #     )

        if current_bar is None or forward_bar is None:
            return
        
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        
        if current_timestamp != forward_timestamp:
            return
        
        multiple_price: MultiplePrice = self._create_multiple_price()
        
        current_month = self.chain.current_month
        
        self._msgbus.publish(topic=self.topic, msg=multiple_price)
        
        has_rolled = current_month != self.chain.current_month
        if has_rolled:
            self._manage_subscriptions()

    def _create_multiple_price(self) -> MultiplePrice:
        
        carry_bar = self.cache.bar(self.carry_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        current_bar = self.cache.bar(self.current_bar_type)
        
        return MultiplePrice(
            bar_type=self.bar_type,
            current_price=Price(current_bar.close, current_bar.close.precision),
            current_bar_type=self.current_bar_type,
            current_month=self.chain.current_month,
            forward_price=Price(forward_bar.close, forward_bar.close.precision)
            if forward_bar is not None
            else None,
            forward_bar_type=self.forward_bar_type,
            forward_month=self.chain.forward_month,
            carry_price=Price(carry_bar.close, carry_bar.close.precision)
            if carry_bar is not None
            else None,
            carry_month=self.chain.carry_month,
            carry_bar_type=self.carry_bar_type,
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )
        
    def _manage_subscriptions(self) -> None:
        
        self._log.debug("Managing subscriptions...")
        
        current_contract = self.chain.current_contract
        forward_contract = self.chain.forward_contract
        carry_contract = self.chain.carry_contract
        
        if self.cache.instrument(current_contract.id) is None:
            self.cache.add_instrument(current_contract)
        
        if self.cache.instrument(forward_contract.id) is None:
            self.cache.add_instrument(forward_contract)
            
        if self.cache.instrument(carry_contract.id) is None:
            self.cache.add_instrument(carry_contract)
        
        self.current_bar_type = BarType(
            instrument_id=self.chain.current_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
        
        self.previous_bar_type = BarType(
            instrument_id=self.chain.previous_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
        
        self.forward_bar_type = BarType(
            instrument_id=self.chain.forward_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
        
        self.carry_bar_type = BarType(
            instrument_id=self.chain.carry_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
        
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        self.unsubscribe_bars(self.previous_bar_type)
        
        # unix_nanos_to_dt(bar.ts_event),
        # bar.bar_type,
        # forward_bar_type,
        # self._chain.roll_date,
        # should_roll,
        # current_timestamp >= self.roll_date,
        # current_day <= self.expiry_day,
        # current_timestamp >= self.roll_date,