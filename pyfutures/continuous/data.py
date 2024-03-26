from collections import deque
import pandas as pd
import pickle

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.bar import ContinuousBar
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.config import RollConfig

class ContractExpired(Exception):
    pass

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        config: RollConfig,
        start_month: ContractMonth,
        maxlen: int = 250,
    ):
        super().__init__()
        self.bar_type = bar_type
        self.roll_offset = config.roll_offset
        self.carry_offset = config.carry_offset
        self.priced_cycle = config.priced_cycle
        self.hold_cycle = config.hold_cycle
        self.approximate_expiry_offset = config.approximate_expiry_offset
        self.start_month = start_month
        self.continuous = deque(maxlen=maxlen)
        self.adjusted: list[float] = []

        assert self.roll_offset <= 0
        assert self.carry_offset == 1 or self.carry_offset == -1
        assert self.start_month in self.hold_cycle
        
        self._maxlen = maxlen
        self._timer_name = f"chain_{self.bar_type}"
    
    @property
    def current_bar_type(self) -> BarType:
        return self._make_bar_type(self.current_month)

    @property
    def previous_month(self) -> ContractMonth:
        return self.hold_cycle.previous_month(self.current_month)
    @property
    def previous_bar_type(self) -> BarType:
        return self._make_bar_type(self.previous_month)
    
    @property
    def forward_month(self) -> ContractMonth:
        return self.hold_cycle.next_month(self.current_month)
        
    @property
    def forward_bar_type(self) -> BarType:
        return self._make_bar_type(self.forward_month)
    
    @property
    def carry_month(self) -> ContractMonth:
        if self.carry_offset == 1:
            return self.priced_cycle.next_month(self.current_month)
        elif self.carry_offset == -1:
            return self.priced_cycle.previous_month(self.current_month)
        
    @property
    def carry_bar_type(self) -> BarType:
        return self._make_bar_type(self.carry_month)
    
    def on_start(self) -> None:
        self.current_month = self.start_month
        self._manage_subscriptions()
        
    def handle_bar(self, bar: Bar) -> None:
        """
        schedule the timer to process the module after x seconds on current or forward bar
        only allow one active timer at once to avoid calculation twice
        """
        is_current = bar.bar_type == self.current_bar_type
        is_forward = bar.bar_type == self.forward_bar_type
        
        if not is_current and not is_forward:
            return
        
        if self._timer_name in self.clock.timer_names:
            return
        
        self.clock.set_time_alert_ns(
            name=self._timer_name,
            alert_time_ns=self.clock.timestamp_ns() + secs_to_nanos(2),
            callback=self._time_event_callback,
        )
    
    def _time_event_callback(self, event: TimeEvent) -> None:
        if self._timer_name in self.clock.timer_names:
            self.clock.cancel_timer(self._timer_name)
        self._handle_time_event(event)
    
    def _handle_time_event(self, _: TimeEvent) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        if current_bar is None:
            return
        
        self._attempt_roll()
        
        start, end = self._roll_window(self.current_month)
        continuous_bar = ContinuousBar(
            bar_type=self.bar_type,
            current_bar=self.cache.bar(self.current_bar_type),
            forward_bar=self.cache.bar(self.forward_bar_type),
            previous_bar=self.cache.bar(self.previous_bar_type),
            carry_bar=self.cache.bar(self.carry_bar_type),
            ts_init=self.clock.timestamp_ns(),
            ts_event=self.clock.timestamp_ns(),
            expiration_ns=dt_to_unix_nanos(end),
            roll_ns=dt_to_unix_nanos(start),
        )
        
        self._handle_continuous_bar(continuous_bar)
        
    def _attempt_roll(self) -> None:
        
        start, end = self._roll_window(self.current_month)
        
        is_expired = self.clock.utc_now() >= end
        if is_expired:
            raise ContractExpired(
                f"The chain failed to roll from {self.current_month} to {self.forward_month} before expiry date {end}",
            )
            
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)
        
        if forward_bar is None:
            return
        
        current_timestamp = unix_nanos_to_dt(current_bar.ts_init)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_init)
        
        if current_timestamp != forward_timestamp:
            return
        
        if current_timestamp >= start and current_timestamp < end:
            self.current_month = self.forward_month  # roll
            self._manage_subscriptions()
                
    def _roll_window(
        self,
        month: ContractMonth,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        # TODO: for live environment the expiry date from the contract should be used
        expiry_date = month.timestamp_utc + pd.Timedelta(days=self.approximate_expiry_offset)
        roll_date = expiry_date + pd.Timedelta(days=self.roll_offset)
        return (roll_date, expiry_date)
    
    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
        Subscribe to previous, current, forward and carry, remove all other subscriptions
        """
        self._log.info("Updating subscriptions...")
        
        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)

    def _make_bar_type(self, month: ContractMonth) -> BarType:
        return BarType(
            instrument_id=self.format_instrument_id(month),
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )
    
    def format_instrument_id(self, month: ContractMonth) -> InstrumentId:
        """
        Format the InstrumentId for contract given the ContractMonth.
        """
        symbol = self.bar_type.instrument_id.symbol.value
        venue = self.bar_type.instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}={month.year}{month.letter_month}.{venue}",
        )
    
    def _update_instruments(self) -> None:
        """
        How to make sure we have the real expiry date from the contract when it calculates?
        The roll attempts needs the expiry date of the current contract.
        The forward contract always cached, therefore the expiry date of the current contract
            will be available directly after a roll.
        Cache the contracts after every roll, and run them on a timer too.
        """
    
    def _handle_continuous_bar(self, bar: ContinuousBar) -> None:
        # most outer layer method for testing purposes
        self.continuous.append(bar)
        self.adjusted = continuous_to_adjusted(list(self.continuous))
        pass
    
def continuous_to_adjusted(bars: list[ContinuousBar]) -> list[float]:
    """
    creating the adjusted from the continuous bars
    iterate over continuous bars backwards
    when it rolls shift the prices by the adjustment value from the bar after the roll
    """
    if len(bars) == 0:
        return []
    elif len(bars) == 1:
        return [float(bars[0].current_bar.close)]
    
    values = deque()
    values.appendleft(bars[-1])
    
    adjustment_value = 0
    
    for i in range(0, len(bars) - 1, -1):
        
        current = bars[i]
        forward = bars[i + 1]
        
        has_rolled = current.current_month != forward.current_month
        if has_rolled:
            adjustment_value = float(current.current_bar.close) - float(current.previous_bar.close)
            
        values.appendleft(
            float(current.current_bar.close) + adjustment_value
        )
        
    return list(values)

class CachedDeque(deque):
    pass
        
class CachedData:
    """
    probably not required? it should save to the cache on_stop
    """
    
    def _update_cache(self, bar: ContinuousBar) -> None:
        
        # append continuous bar to the cache
        bars = self._load_continuous_bars()
        bars.append(bar)
        bars = bars[-self._maxlen:]
        assert len(bars) <= self._maxlen
        
        self._save_continuous_bars(bars)
        
        # update the adjusted series to the cache
        adjusted: list[float] = self._calculate_adjusted(bars)
        self._save_adjusted(adjusted)
    
    def _load_continuous_bars(self) -> list[ContinuousBar]:
        key = str(self.bar_type)
        data: bytes | None = self.cache.get(key)
        if data is None:
            return []
        data: list[dict] = pickle.loads(data)
        return [ContinuousBar.from_dict(b) for b in data]
        
    def _save_continuous_bars(self, bars: list[ContinuousBar]) -> None:
        bars: list[dict] = [ContinuousBar.to_dict(b) for b in bars]
        data: bytes = pickle.dumps(bars)
        key = str(self.bar_type)
        self.cache.add(key, data)
    
    def _save_adjusted(self, adjusted: list[float]) -> None:
        data: bytes = pickle.dumps(adjusted)
        key = f"{self.bar_type}a"
        self.cache.add(key, data)
        
    def _load_adjusted(self) -> list[float]:
        key = f"{self.bar_type}a"
        data: bytes = self.cache.get(key, data)
        return pickle.loads(data)
        

        
    # interval = self.bar_type.spec.timedelta
    # now = unix_nanos_to_dt(self.clock.timestamp_ns())
    # start_time = now.floor(interval) - interval + pd.Timedelta(seconds=5)

    # self.clock.set_timer(
    #     name=f"chain_{self.bar_type}",
    #     interval=interval,
    #     start_time=start_time,
    #     callback=self._handle_time_event,
    # )

    
        
        
        # self.msgbus.publish(
        #     topic=str(self.bar_type),
        #     msg=bar,
        # )

        # self.current_bars.append(self.current_bar)

        # self.msgbus.publish(
        #     topic=self.topic,
        #     msg=self.current_bar,
        # )
        
        # self.adjusted.append(float(self.current_bar.close))
        # self.topic = f"data.bars.{self.bar_type}"
        
        # current_bar = self.cache.bar(self.current_bar_type)
        # is_last = self._last_current is not None and self._last_current == current_bar
        # self._last_current = current_bar

        # if is_last:
        #     return
        # self._last_current: Bar | None = None