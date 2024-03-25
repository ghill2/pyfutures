import pandas as pd
from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig


class ContractExpired(Exception):
    pass


class ContinuousData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain_config: ContractChainConfig,
    ):
        super().__init__()
        self.bar_type = bar_type
        self._chain = ContractChain(config=chain_config, clock=self._clock)

    @property
    def current_bar_type(self) -> BarType:
        return self._make_bar_type(self._chain.current_contract_id)

    @property
    def previous_bar_type(self) -> BarType:
        return self._make_bar_type(self._chain.previous_contract_id)

    @property
    def forward_bar_type(self) -> BarType:
        return self._make_bar_type(self._chain.forward_contract_id)

    @property
    def carry_bar_type(self) -> BarType:
        return self._make_bar_type(self._chain.current_contract_id)

    @property
    def current_bar(self) -> Bar:
        return self.cache.bar(self.current_bar_type, 0)

    @property
    def forward_bar(self) -> Bar:
        return self.cache.bar(self.forward_bar_type, 0)

    @property
    def carry_bar(self) -> Bar:
        return self.cache.bar(self.carry_bar_type, 0)

    @property
    def previous_bar(self) -> Bar:
        return self.cache.bar(self.previous_bar_type, 0)

    def on_start(self) -> None:
        self._chain.start()

        interval = self.bar_type.spec.timedelta
        now = unix_nanos_to_dt(self.clock.timestamp_ns())
        start_time = now.floor(interval) - interval + pd.Timedelta(seconds=5)

        self.clock.set_timer(
            name=f"chain_{self.bar_type}",
            interval=interval,
            start_time=start_time,
            callback=self._handle_time_event,
        )

    def _handle_time_event(self, event: TimeEvent) -> None:
        # manage subscriptions
        # TODO: self._update_subscriptions()

        is_expired = self.clock.utc_now() >= self._chain.expiry_date
        if is_expired:
            raise ContractExpired(
                f"The chain failed to roll from {self._chain.current_month} to {self._chain.forward_month} before expiry date {self._chain.expiry_date}",
            )

        self._attempt_roll()

    def _attempt_roll(self) -> None:
        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if current_bar is None or forward_bar is None:
            return

        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)

        if current_timestamp != forward_timestamp:
            return

        in_roll_window = (current_timestamp >= self._chain.roll_date) and (current_timestamp < self._chain.expiry_date)

        if not in_roll_window:
            return

        self._chain.roll()

    def _manage_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
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
