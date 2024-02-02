from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from collections import deque
from pyfutures.continuous.actor import ChainActor
    
class ContinuousData(ChainActor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
        handler: Callable | None = None,
        lookback: int | None = 10_000,
    ):
        super().__init__(
            bar_type=bar_type,
            chain=chain,
        )
        
        self.recv_count = 0
        self.prices = deque(maxlen=lookback)
        self._handler = handler
        
    def on_bar(self, bar: Bar) -> None:
        
        current_bar = self.current_bar
        
        if bar.bar_type != self.current_bar_type or self.current_bar is None:
            self.recv_count += 1
            return
        
        forward_bar = self.forward_bar
        carry_bar = self.carry_bar
        
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
        
        self.recv_count += 1
    