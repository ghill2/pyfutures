from collections import deque
import pickle

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.bar import ContinuousBar

class ContractExpired(Exception):
    pass

class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
        maxlen: int = 250,
    ):
        super().__init__()
        self.bar_type = bar_type
        self.chain = chain
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

    def _handle_time_event(self, event: TimeEvent) -> None:
        
        is_expired = self.clock.utc_now() >= self.chain.expiry_date
        if is_expired:
            raise ContractExpired(
                f"The chain failed to roll from {self.chain.current_month} to {self.chain.forward_month} before expiry date {self.chain.expiry_date}",
            )

        should_roll: bool = self._should_roll()
        if should_roll:
            self.chain.roll()
            self._manage_subscriptions()
            # self._adjust()
        
        current_bar = self.cache.bar(self.current_bar_type)
        is_last = self._last_current is not None and self._last_current == current_bar
        self._last_current = current_bar

        if is_last:
            return
        
        self._append_continuous_bar()
        
    def _append_continuous_bar(self) -> None:
        
        current_bar = self.cache.bar(self.current_bar_type)
        assert current_bar is not None  # design-time error
        
        bar = ContinuousBar(
            bar_type=self.bar_type,
            current_bar=current_bar,
            forward_bar=self.cache.bar(self.forward_bar_type),
            previous_bar=self.cache.bar(self.previous_bar_type),
            carry_bar=self.cache.bar(self.carry_bar_type),
            ts_init=self.clock.timestamp_ns(),
            ts_event=self.clock.timestamp_ns(),
        )
        
        bars = self._load_continuous_bars()
        bars.append(bar)
        bars = bars[-self._maxlen:]
        assert len(bars) <= self._maxlen
        
        self._save_continuous_bars(bars)
        adjusted: list[float] = self._calculate_adjusted(bars)
        self._save_adjusted(adjusted)
    
    def _load_continuous_bars(self) -> list[ContinuousBar]:
        key = str(self.bar_type)
        data: bytes = self.cache.get(key)
        data: list[dict] = pickle.loads(data)
        return [ContinuousBar.from_dict(b) for b in data]
        
    def _save_continuous_bars(self, bars: list[ContinuousBar]) -> None:
        bars: list[dict] = [ContinuousBar.to_dict(b) for b in bars]
        data: bytes = pickle.dumps(bars)
        
        key = str(self.bar_type)
        self.cache.set(key, data)
    
    def _calculate_adjusted(self, bars: list[ContinuousBar]) -> list[float]:
        """
        remaking the adjusted from the continuous bars
        iterate over continuous bars backwards
        when it rolls shift the prices by the adjustment value from the bar after the roll
        """
        pass
    
    def _save_adjusted(self, adjusted: list[float]) -> None:
        data: bytes = pickle.dumps(adjusted)
        key = f"{self.bar_type}a"
        self.cache.set(key, data)
        
    def _should_roll(self) -> bool:
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if current_bar is None or forward_bar is None:
            return False

        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)

        if current_timestamp != forward_timestamp:
            return False

        in_roll_window = (current_timestamp >= self.chain.roll_date) and (current_timestamp < self.chain.expiry_date)

        return in_roll_window
        
        # adjustment_value = float(self.current_bar.close) - float(self.previous_bar.close)
        # self.adjusted = deque(
        #     [x + adjustment_value for x in self.adjusted],
        #     maxlen=self.adjusted.maxlen,
        # )
        
    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
        Subscribe to previous, current, forward and carry, remove all other subscriptions
        """
        self._log.info("Updating subscriptions...")
        
        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)

    def _update_instruments(self) -> None:
        """
        How to make sure we have the real expiry date from the contract when it calculates?
        The roll attempts needs the expiry date of the current contract.
        The forward contract always cached, therefore the expiry date of the current contract
            will be available directly after a roll.
        Cache the contracts after every roll, and run them on a timer too.
        """

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