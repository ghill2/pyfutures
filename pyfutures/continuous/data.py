from __future__ import annotations

from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType

from pyfutures.continuous.actor import Actor
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.chain import RollEvent
from pyfutures.continuous.multiple_bar import MultipleBar


class MultipleData(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
    ):
        super().__init__()

        self.bar_type = bar_type
        self.instrument_id = bar_type.instrument_id
        self.topic = f"{self.bar_type}0"
        self.chain = chain

    def on_start(self) -> None:
        self.msgbus.subscribe(
            topic="events.roll",
            handler=self._handle_roll_event,
            # TODO determine priority
        )
        
        self._manage_subscriptions()

    def _handle_roll_event(self, event: RollEvent) -> None:
        if event.bar_type == self.chain.bar_type:
            self._manage_subscriptions()

    def on_bar(self, bar: Bar) -> None:
        is_forward_or_current = bar.bar_type == self.current_bar_type or bar.bar_type == self.forward_bar_type

        if not is_forward_or_current:
            return

        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        # # for debugging
        # if "DAY" in str(self.bar_type) and self.chain.current_month.value == "2008M":
        #     # self._log.debug(repr(bar))
        #     current_timestamp_str = str(unix_nanos_to_dt(current_bar.ts_event))[:-6] if current_bar is not None else None
        #     forward_timestamp_str = str(unix_nanos_to_dt(forward_bar.ts_event))[:-6] if forward_bar is not None else None
        #     print(
        #         f"{self.chain.current_month.value} {current_timestamp_str} "
        #         f"{self.chain.forward_month.value} {forward_timestamp_str} "
        #         f"{str(self.chain.roll_date)[:-15]} "
        #         f"{str(self.chain.expiry_date)[:-15]} "
        #     )
        
        # calculate the strategy on every current_bar
        # roll first time current_bar and forward_bar has same timestamp and is within roll window
        
        # strategy calculates, create new position size, submit new position size to forward contract 
        # needs to use forward price
        # add liquid window for execution
        
        if current_bar is None or forward_bar is None:
            return

        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)

        if current_timestamp != forward_timestamp:
            return

        multiple_bar = MultipleBar(
            bar_type=self.bar_type,
            current_bar=current_bar,
            forward_bar=forward_bar,
            carry_bar=self.cache.bar(self.carry_bar_type),
            ts_event=current_bar.ts_event,
            ts_init=current_bar.ts_init,
        )

        self._msgbus.publish(topic=self.topic, msg=multiple_bar)

    def _manage_subscriptions(self) -> None:
        self._log.debug("Managing subscriptions...")

        current_contract = self.chain.current_contract
        forward_contract = self.chain.forward_contract
        carry_contract = self.chain.carry_contract

        if self.cache.instrument(current_contract.id) is None:
            self.cache.add_instrument(current_contract)

        if self.cache.instrument(forward_contract.id) is None:
            self.cache.add_instrument(forward_contract)

        if self.cache.instrument(carry_contract.id) is None:
            self.cache.add_instrument(carry_contract)

        self.current_bar_type = BarType(
            instrument_id=self.chain.current_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.previous_bar_type = BarType(
            instrument_id=self.chain.previous_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.forward_bar_type = BarType(
            instrument_id=self.chain.forward_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.carry_bar_type = BarType(
            instrument_id=self.chain.carry_contract.id,
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)
        self.unsubscribe_bars(self.previous_bar_type)

        # unix_nanos_to_dt(bar.ts_event),
        # bar.bar_type,
        # forward_bar_type,
        # self._chain.roll_date,
        # should_roll,
        # current_timestamp >= self.roll_date,
        # current_day <= self.expiry_day,
        # current_timestamp >= self.roll_date,
