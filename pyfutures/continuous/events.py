from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import InstrumentId


class RollEvent(Event):
    def __init__(
        self,
        ts_init: int,
        from_instrument_id: InstrumentId,
        to_instrument_id: InstrumentId,
    ):
        self.from_instrument_id = from_instrument_id
        self.to_instrument_id = to_instrument_id

        self._event_id = UUID4()
        self._ts_event = ts_init  # Timestamp identical to ts_init
        self._ts_init = ts_init

    @property
    def id(self) -> UUID4:
        return self._event_id

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init
