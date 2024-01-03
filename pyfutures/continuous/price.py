from __future__ import annotations

import pyarrow as pa

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from nautilus_trader.model.continuous.contract_month import ContractMonth
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price


class ContinuousPrice(Data):
    def __init__(
        self,
        instrument_id: InstrumentId,
        forward_price: Price | None,
        forward_month: ContractMonth,
        current_price: Price,
        current_month: ContractMonth,
        carry_price: Price | None,
        carry_month: ContractMonth,
        ts_event: int,
        ts_init: int,
    ):
        super().__init__()

        self._instrument_id = instrument_id
        self._current_price = current_price
        self._current_month = current_month
        self._forward_price = forward_price
        self._forward_month = forward_month
        self._carry_price = carry_price
        self._carry_month = carry_month
        self._ts_event = ts_event
        self._ts_init = ts_init

    @property
    def instrument_id(self) -> InstrumentId:
        return self._instrument_id

    @property
    def current_price(self) -> Price:
        return self._current_price

    @property
    def current_month(self) -> ContractMonth:
        return self._current_month

    @property
    def forward_price(self) -> Price:
        return self._forward_price

    @property
    def forward_month(self) -> ContractMonth:
        return self._forward_month

    @property
    def carry_price(self) -> Price:
        return self._carry_price

    @property
    def carry_month(self) -> ContractMonth:
        return self._carry_month

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ContinuousPrice):
            return False
        return (
            self._instrument_id == other.instrument_id
            and self._current_price == other._current_price
            and self._current_month == other._current_month
            and self._forward_price == other._forward_price
            and self._forward_month == other._forward_month
            and self._carry_price == other.carry_price
            and self._carry_month == other._carry_month
            and self._ts_event == other.ts_event
            and self._ts_init == other.ts_init
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"instrument_id={self._instrument_id}, "
            f"current_price={self._current_price}, "
            f"current_month={self._current_month}, "
            f"forward_price={self._forward_price}, "
            f"forward_month={self._forward_month}, "
            f"carry_price={self._carry_price}, "
            f"carry_month={self._carry_month}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )

    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("instrument_id", pa.dictionary(pa.int64(), pa.string())),
                pa.field("current_price", pa.string()),
                pa.field("current_month", pa.string()),
                pa.field("forward_price", pa.string(), nullable=True),
                pa.field("forward_month", pa.string()),
                pa.field("carry_price", pa.string(), nullable=True),
                pa.field("carry_month", pa.string()),
                pa.field("ts_event", pa.uint64()),
                pa.field("ts_init", pa.uint64()),
            ],
        )

    @staticmethod
    def to_dict(obj: ContinuousPrice) -> dict:
        return {
            "instrument_id": obj.instrument_id.value,
            "current_price": str(obj._current_price),
            "current_month": obj._current_month.value,
            "forward_price": str(obj._forward_price),
            "forward_month": obj._forward_month.value,
            "carry_price": str(obj.carry_price),
            "carry_month": obj._carry_month.value,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }

    @staticmethod
    def from_dict(values: dict) -> ContinuousPrice:
        PyCondition.not_none(values, "values")
        return ContinuousPrice(
            instrument_id=InstrumentId.from_str(values["instrument_id"]),
            current_price=Price.from_str(values["current_price"]),
            current_month=ContractMonth(values["current_month"]),
            forward_price=Price.from_str(values["forward_price"])
            if values.get("forward_price") is not None
            else None,
            forward_month=ContractMonth(values["forward_month"]),
            carry_price=Price.from_str(values["carry_price"])
            if values.get("carry_price") is not None
            else None,
            carry_month=ContractMonth(values["carry_month"]),
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    def __getstate__(self):
        return (
            self.instrument_id.value,
            str(self.current_price),
            self._current_month.value,
            str(self._forward_price),
            self._forward_month.value,
            str(self.carry_price),
            self.carry_month.value,
            self.ts_event,
            self.ts_init,
        )

    def __setstate__(self, state):
        self._instrument_id = InstrumentId.from_str(state[0])
        self._current_price = Price.from_str(state[1])
        self._current_month = ContractMonth(state[2])
        self._forward_price = Price.from_str(state[3])
        self._forward_month = ContractMonth(state[4])
        self._carry_price = Price.from_str(state[5])
        self._carry_month = ContractMonth(state[6])
        self._ts_event = state[7]
        self._ts_init = state[8]
