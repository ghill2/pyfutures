from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from nautilus_trader.common.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.enums import BarAggregation

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
        start_month: ContractMonth,
        # end_month: ContractMonth | None,
        handler: Callable | None = None,
        
    ):
        super().__init__()
        self.recv_count = 0
        
        self._bar_type = bar_type
        self._chain = chain
        self._instrument_id = bar_type.instrument_id
        self._handler = handler
        self._start_month = start_month
                
    def on_start(self) -> None:
        
        self.current_contract = self._chain.current_contract(self._start_month)
        self.roll()

    def on_bar(self, bar: Bar) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
            
        if bar.bar_type == self.current_bar_type and current_bar is not None:
            self._send_multiple_price()
        
        self.recv_count += 1
            
    def _send_multiple_price(self) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        carry_bar = self.cache.bar(self.carry_bar_type)
        
        multiple_price = MultiplePrice(
            bar_type=self._bar_type,
            current_price=Price(current_bar.close, current_bar.close.precision),
            current_month=self.current_contract.info["month"],
            forward_price=Price(forward_bar.close, forward_bar.close.precision)
            if forward_bar is not None
            else None,
            forward_month=self.forward_contract.info["month"],
            carry_price=Price(carry_bar.close, carry_bar.close.precision)
            if carry_bar is not None
            else None,
            carry_month=self.carry_contract.info["month"],
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )

        if self._handler is not None:
            self._handler(multiple_price)

        self._msgbus.publish(topic=f"{self._bar_type}0", msg=multiple_price)

    def roll(self) -> None:
        
        self._chain.roll()
        
        self.current_bar_type = BarType(
            instrument_id=self.current_contract.id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
        # TODO: unsubscribe to old bars self.unsubscribe_bars(self.current_bar_type)
        
        self.forward_bar_type = BarType(
            instrument_id=self.forward_contract.id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

        self.carry_bar_type = BarType(
            instrument_id=self.carry_contract.id,
            bar_spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
        
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        self.subscribe_bars(self.carry_bar_type)
        
        print(self.current_contract.info["month"])