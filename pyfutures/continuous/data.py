from collection import deque
from nautilus_trader.common.actor import Actor


class ContractExpired(Exception):
    pass


class ContinuousData(Actor):
    def __init__(self):
        self.continuous = deque(maxlen=maxlen)
        self.adjusted: list[float] = []

    # self._update_instruments()

    # def _update_instruments(self) -> None:
    #     """
    #     How to make sure we have the real expiry date from the contract when it calculates?
    #     The roll attempts needs the expiry date of the current contract.
    #     The forward contract always cached, therefore the expiry date of the current contract
    #         will be available directly after a roll.
    #     Cache the contracts after every roll, and run them on a timer too.
    #     """

    #     self._log.info("Updating instruments...")

    #     instrument_ids = [
    #         self.current_bar_type.instrument_id,
    #         self.previous_bar_type.instrument_id,
    #         self.forward_bar_type.instrument_id,
    #         self.carry_bar_type.instrument_id,
    #     ]

    #     for instrument_id in instrument_ids:
    #         # if self.instrument_provider.find(instrument_id) is None:
    #         self.instrument_provider.load(instrument_id)

    #     for instrument in self.instrument_provider.list_all():
    #         if instrument.id not in self.cache:
    #             self.cache.add_instrument(instrument)

    # def _handle_continuous_bar(self, bar: ContinuousBar) -> None:
    #     # most outer layer method for testing purposes
    #     self.continuous.append(bar)
    #     self.adjusted = self.continuous_to_adjusted(list(self.continuous))

    # continuous_bar = ContinuousBar(
    #     bar_type=self.bar_type,
    #     current_bar=self.cache.bar(self.current_bar_type),
    #     forward_bar=self.cache.bar(self.forward_bar_type),
    #     previous_bar=self.cache.bar(self.previous_bar_type),
    #     carry_bar=self.cache.bar(self.carry_bar_type),
    #     ts_init=self.clock.timestamp_ns(),
    #     ts_event=self.clock.timestamp_ns(),
    #     expiration_ns=dt_to_unix_nanos(end),
    #     roll_ns=dt_to_unix_nanos(start),
    # )

    # self._handle_continuous_bar(continuous_bar)

    # def on_load(self, state: dict) -> None:
    #     self.continuous.extendleft(state["continuous"])
    #     self.adjusted = state["adjusted"]

    # def on_save(self) -> dict:
    #     # TODO: do I need to rename this to make it unique per ContinuousData instance?
    #     return {
    #         "continuous": self.continuous_bars_to_bytes(self.continuous),
    #         "adjusted": pickle.dumps(self.adjusted),
    #     }

    # @staticmethod
    # def continuous_bars_to_bytes(bars: list[ContinuousBar]) -> bytes:
    #     bars: list[dict] = [ContinuousBar.to_dict(b) for b in bars]
    #     return pickle.dumps(bars)

    # @staticmethod
    # def bytes_to_continuous_bars(data: bytes) -> list[ContinuousBar]:
    #     data: list[dict] = pickle.loads(data)
    #     return [ContinuousBar.from_dict(b) for b in data]


# class DataConverter:
#     """
#     transforms data to bytes for storage in the cache
#     """
#     def _load_continuous_bars(self) -> list[ContinuousBar]:
#         key = str(self.bar_type)
#         data: bytes | None = self.cache.get(key)
#         if data is None:
#             return []
#         data: list[dict] = pickle.loads(data)
#         return [ContinuousBar.from_dict(b) for b in data]

#     def _save_continuous_bars(self, bars: list[ContinuousBar]) -> None:

#         key = str(self.bar_type)
#         self.cache.add(key, data)

#     def _save_adjusted(self, adjusted: list[float]) -> None:
#         data: bytes = pickle.dumps(adjusted)
#         key = f"{self.bar_type}a"
#         self.cache.add(key, data)

#     def _load_adjusted(self) -> list[float]:
#         key = f"{self.bar_type}a"
#         data: bytes = self.cache.get(key, data)
#         return pickle.loads(data)

# def _update_cache(self, bar: ContinuousBar) -> None:

#     # append continuous bar to the cache
#     bars = self._load_continuous_bars()
#     bars.append(bar)
#     bars = bars[-self._maxlen:]
#     assert len(bars) <= self._maxlen

#     self._save_continuous_bars(bars)

#     # update the adjusted series to the cache
#     adjusted: list[float] = self._calculate_adjusted(bars)
#     self._save_adjusted(adjusted)

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
