from typing import Callable

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

class RollSignal:
    def __init__(
        self,
        chain: ContractChain,
        handlers: set[Callable],
        raise_expired: bool = True,
        ignore_expiry_date: bool = False,
        manual_roll: bool = False,
    ):
    
    def on_start(self) -> None:
        self.current_contract = self._chain.current_contract(self._start_month)
        self.roll()
        
    def on_bar(self, bar: Bar) -> None:
        
        if bar.bar_type != self.current_bar_type and bar.bar_type != self.forward_bar_type:
            return False
        
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        # next bar arrived before current or vice versa
        if current_bar is None or forward_bar is None:
            return
        
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
    
        if self._raise_expired:
            is_expired = unix_nanos_to_dt(bar.ts_event) >= (self.expiry_day + pd.Timedelta(days=1))
            if is_expired:
                # TODO: wait for next forward bar != last timestamp
                raise ValueError(f"ContractExpired {self.current_id}")
        
        # in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)
        if self._ignore_expiry_date:
            in_roll_window = (current_timestamp >= self.roll_date)
        else:
            current_day = current_timestamp.floor("D")
            in_roll_window = (current_timestamp >= self.roll_date) and (current_day <= self.expiry_day)
        
        # a valid roll time is where both timestamps are equal
        should_roll = in_roll_window and current_timestamp == forward_timestamp
        if should_roll:
            for handler in self._handlers:
                handler()
        
    # # for debugging
    # if "MINUTE" in str(self._bar_type) and self.current_contract.info["month"].value == "1998X":
    #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
    #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
    #     print(
    #         f"{self.current_contract.info.get('month').value} {current_timestamp_str}",
    #         f"{self.forward_contract.info.get('month').value} {forward_timestamp_str}",
    #         str(self.roll_date)[:-15],
    #         str(self.expiry_date)[:-15],
    #         bar.bar_type == self.current_bar_type,
    #         current_bar is not None,
    #         self.roll_date,
    #         # should_roll,
    #         # current_timestamp >= self.roll_date,
    #         # current_day <= self.expiry_day,
    #         # current_timestamp >= self.roll_date,
    #     )