from typing import Callable

import pandas as pd

from nautilus_trader.common.actor import Actor
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType

# from pyfutures.continuous.actor import ChainActor
from pyfutures.continuous.price import MultiplePrice
class RollSignal(Actor):
    """
    Listens to contract bars and triggers a roll event
    """
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
        raise_expired: bool = True,
        ignore_expiry_date: bool = False,
    ):
        
        super().__init__()
        self._bar_type = bar_type
        self._chain = chain
        self._raise_expired = raise_expired
        self._ignore_expiry_date = ignore_expiry_date
    
    def on_start(self) -> None:
        # TODO: handle subscribe and unsubscribe of current and forward contracts
        self.msgbus.subscribe(
            topic=f"{self._bar_type}0",
            handler=self.on_multiple_price,
        )
        # subscribe_data
    
    def on_multiple_price(self, price: MultiplePrice) -> None:
        
        expiry_day = self._chain.expiry_date.floor("D")
        roll_date = self._chain.roll_date
        current_timestamp = unix_nanos_to_dt(price.ts_event)
        
        if not self._ignore_expiry_date:
            is_expired = current_timestamp >= (expiry_day + pd.Timedelta(days=1))
            if is_expired and self._raise_expired:
                # TODO: wait for next forward bar != last timestamp
                raise ValueError(f"ContractExpired {self._bar_type}")
        
        if self._ignore_expiry_date:
            in_roll_window = (current_timestamp >= roll_date)
        else:
            current_day = current_timestamp.floor("D")
            in_roll_window = (current_timestamp >= roll_date) and (current_day <= expiry_day)
        
        if in_roll_window:
            # TODO: self.msgbus.topics, handle subscribe and unsubscribe of current and forward contracts
            self._chain.roll()
        
    
    
    # current_bar_type = self.current_bar_type
    # forward_bar_type = self.forward_bar_type
    
    # if bar.bar_type == current_bar_type or bar.bar_type == forward_bar_type:
    #     pass
    # else:
    #     return
    
    # current_bar = self.cache.bar(current_bar_type)
    # forward_bar = self.cache.bar(forward_bar_type)
    
    # # for debugging
    # if self._chain.current_month.value == "2012X":
    #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
    #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
    #     print(
    #         f"{self._chain.current_month.value} {current_timestamp_str}",
    #         f"{self._chain.forward_month.value} {forward_timestamp_str}",
    #         str(self._chain.roll_date)[:-15],
    #         str(self._chain.expiry_date)[:-15],
    #         # unix_nanos_to_dt(bar.ts_event),
    #         # bar.bar_type,
    #         # forward_bar_type,
    #         # self._chain.roll_date,
    #         # should_roll,
    #         # current_timestamp >= self.roll_date,
    #         # current_day <= self.expiry_day,
    #         # current_timestamp >= self.roll_date,
    #     )
        
    # # next bar arrived before current or vice versa
    # if current_bar is None or forward_bar is None:
    #     return
    
    # current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
    # forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
    
# in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)