from collections.abc import Callable

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.model.data import Bar

from pyfutures.continuous.chain import ContractChain


class ContinuousBarAggregator(Actor):
    """
    Waits for a specified seconds to capture current and forward
    """

    def __init__(
        self,
        chain: ContractChain,
        callback: Callable,
        wait_seconds: float = 2,
    ):
        self._chain = chain
        self._callback = callback
        self._wait_seconds = wait_seconds
        self._timer_name = f"chain_{chain.bar_type}"

    def handle_bar(self, bar: Bar) -> None:
        """
        schedule the timer to process the module after x seconds on current or forward bar
        only allow one active timer at once to avoid calculation twice
        """
        is_current = bar.bar_type == self._chain.current_bar_type
        is_forward = bar.bar_type == self._chain.forward_bar_type

        if not is_current and not is_forward:
            return

        if self._timer_name in self.clock.timer_names:
            return

        self.clock.set_time_alert_ns(
            name=self._timer_name,
            alert_time_ns=self.clock.timestamp_ns() + secs_to_nanos(self._wait_seconds),
            callback=self._time_event_callback,
        )

    def _time_event_callback(self, event: TimeEvent) -> None:
        if self._timer_name in self.clock.timer_names:
            self.clock.cancel_timer(self._timer_name)
        self._callback()
        self._manage_subscriptions()

    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
        Subscribe to previous, current, forward and carry, remove all other subscriptions
        """
        self._log.info("Managing subscriptions...")

        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
