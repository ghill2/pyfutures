from collections import deque

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.chain import ContractChain


class ContractExpired(Exception):
    pass


class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
    ):
        super().__init__()
        self.bar_type = bar_type
        self.chain = chain
        self.adjusted = deque(maxlen=None)
        self.topic = f"data.bars.{self.bar_type}"
        self.current_bars = deque(maxlen=None)

        self._last_current: Bar | None = None

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
    def current_bar(self) -> Bar:
        return self.cache.bar(self.current_bar_type, 0)

    @property
    def previous_bar(self) -> Bar:
        return self.cache.bar(self.previous_bar_type, 0)

    def handle_bar(self, bar: Bar) -> None:
        if bar.bar_type != self.current_bar_type:
            return

        # schedule the timer to process the module after x seconds
        # to wait for other bars to arrive
        self.clock.set_time_alert_ns(
            name=f"chain_{self.bar_type}_{UUID4()}",
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
            self._adjust()

        is_last = self._last_current is not None and self._last_current == self.current_bar
        self._last_current = self.current_bar

        if is_last:
            return

        self.adjusted.append(float(self.current_bar.close))

        assert self.current_bar is not None  # design-time error

        self.current_bars.append(self.current_bar)

        self.msgbus.publish(
            topic=self.topic,
            msg=self.current_bar,
        )

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

    def _adjust(self):
        adjustment_value = float(self.current_bar.close) - float(self.previous_bar.close)
        self.adjusted = deque(
            [x + adjustment_value for x in self.adjusted],
            maxlen=self.adjusted.maxlen,
        )

    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
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
