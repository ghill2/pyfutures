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

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        roll_config: RollConfig,
        maxlen: int = 250,
    ):
        super().__init__()
        self.bar_type = bar_type
        self.roll_offset = config.roll_config.roll_offset
        self.carry_offset = config.roll_config.carry_offset
        self.priced_cycle = config.roll_config.priced_cycle
        self.hold_cycle = config.roll_config.hold_cycle
        self.approximate_expiry_offset = config.roll_config.approximate_expiry_offset
        self.start_month = config.start_month

        assert self.roll_offset <= 0
        assert self.carry_offset == 1 or self.carry_offset == -1
        assert self.start_month in self.hold_cycle
        
        self._last_current: Bar | None = None
        self._maxlen = maxlen

    @property
    def current_bar_type(self) -> BarType:
        return self._make_bar_type(self.chain.current_contract_id)

    @property
    def previous_bar_type(self) -> BarType:
        return self._make_bar_type(self.chain.previous_contract_id)

    @property
    def forward_bar_type(self) -> BarType:
        return self._make_bar_type(self.chain.forward_contract_id)

    @property
    def carry_bar_type(self) -> BarType:
        return self._make_bar_type(self.chain.carry_contract_id)
    
    @property
    def continuous_bars(self) -> list[ContinuousBar]:
        return self._load_continuous_bars()
    
    @property
    def adjusted(self) -> list[float]:
        return self._load_adjusted()
    
    def handle_bar(self, bar: Bar) -> None:
        """
        schedule the timer to process the module after x seconds on current or forward bar
        only allow one active timer at once to avoid calculation twice
        """
        is_current = bar.bar_type == self.current_bar_type
        is_forward = bar.bar_type == self.forward_bar_type
        
        if not is_current and not is_forward:
            return
        
        name = f"chain_{self.bar_type}"
        if name in self.clock.timer_names:
            return
        
        self.clock.set_time_alert_ns(
            name=name,
            alert_time_ns=self.clock.timestamp_ns() + secs_to_nanos(2),
            callback=self._handle_time_event,
        )

    def on_start(self) -> None:
        assert len(self.chain.rolls) > 0
        self._manage_subscriptions()
    
    @property
    def current_month(self):
        
        bars = self.continuous_bars
        if len(bars) == 0:
            return self._start_month
        return self.continuous_bars[-1].current_month
        
    def _handle_time_event(self, event: TimeEvent) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        if current_bar is None:
            return
        
        expiry_date = self.current_month.timestamp_utc + pd.Timedelta(days=self.approximate_expiry_offset)
        roll_date = expiry_date + pd.Timedelta(days=self.roll_offset)
        
        continuous_bar = ContinuousBar(
            bar_type=self.bar_type,
            current_bar=current_bar,
            forward_bar=self.cache.bar(self.forward_bar_type),
            previous_bar=self.cache.bar(self.previous_bar_type),
            carry_bar=self.cache.bar(self.carry_bar_type),
            ts_init=self.clock.timestamp_ns(),
            ts_event=self.clock.timestamp_ns(),
            expiration_ns=dt_to_unix_nanos(expiry_date),
            roll_ns=dt_to_unix_nanos(expiry_date),
        )
        self._handle_continuous_bar(continuous_bar)
        
    def _handle_continuous_bar(self, bar: ContinuousBar) -> None:
        self.chain.attempt_roll(bar)
        self._manage_subscriptions()
        self._update_cache(bar)
    
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
    
    def _calculate_adjusted(self, bars: list[ContinuousBar]) -> list[float]:
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
    
    def _save_adjusted(self, adjusted: list[float]) -> None:
        data: bytes = pickle.dumps(adjusted)
        key = f"{self.bar_type}a"
        self.cache.add(key, data)
        
    def _load_adjusted(self) -> list[float]:
        key = f"{self.bar_type}a"
        data: bytes = self.cache.get(key, data)
        return pickle.loads(data)
        
    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
        Subscribe to previous, current, forward and carry, remove all other subscriptions
        """
        self._log.info("Updating subscriptions...")
        
        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)

    def _make_bar_type(self, instrument_id: InstrumentId) -> BarType:
        return BarType(
            instrument_id=instrument_id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

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