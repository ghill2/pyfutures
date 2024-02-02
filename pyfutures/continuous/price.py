from __future__ import annotations

import pyarrow as pa

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.objects import Price
from nautilus_trader.model.data import BarType

class MultiplePrice(Data):
    def __init__(
        self,
        bar_type: BarType,
        forward_price: Price | None,
        forward_bar_type: BarType,
        current_price: Price,
        current_bar_type: BarType,
        carry_price: Price | None,
        carry_bar_type: BarType,
        ts_event: int,
        ts_init: int,
    ):
        super().__init__()

        self.bar_type = bar_type
        self.instrument_id = bar_type.instrument_id
        self.current_price = current_price
        self.current_bar_type = current_bar_type
        self.forward_price = forward_price
        self.forward_bar_type = forward_bar_type
        self.carry_price = carry_price
        self.carry_bar_type = carry_bar_type
        self._ts_event = ts_event
        self._ts_init = ts_init

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init

    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("current_price", pa.string()),
                pa.field("current_bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("forward_price", pa.string(), nullable=True),
                pa.field("forward_bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("carry_price", pa.string(), nullable=True),
                pa.field("carry_bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("ts_event", pa.uint64()),
                pa.field("ts_init", pa.uint64()),
            ],
        )

    @staticmethod
    def to_dict(obj: MultiplePrice) -> dict:
        return {
            "bar_type": str(obj.bar_type),
            "current_price": str(obj.current_price),
            "current_bar_type": str(obj.current_bar_type),
            "forward_price": str(obj.forward_price) if obj.forward_price is not None else None,
            "forward_bar_type": str(obj.forward_bar_type),
            "carry_price": str(obj.carry_price) if obj.carry_price is not None else None,
            "carry_bar_type": str(obj.carry_bar_type),
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }

    @staticmethod
    def from_dict(values: dict) -> MultiplePrice:
        PyCondition.not_none(values, "values")
        return MultiplePrice(
            bar_type=BarType.from_str(values["bar_type"]),
            current_price=Price.from_str(values["current_price"]),
            current_bar_type=BarType.from_str(values["current_bar_type"]),
            forward_price=Price.from_str(values["forward_price"])
            if values.get("forward_price") is not None
            else None,
            forward_bar_type=BarType.from_str(values["forward_bar_type"]),
            carry_price=Price.from_str(values["carry_price"])
            if values.get("carry_price") is not None
            else None,
            carry_bar_type=BarType.from_str(values["carry_bar_type"]),
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    def __getstate__(self) -> tuple:
        return tuple(self.to_dict(self).values())

    def __setstate__(self, state):
        self.bar_type = BarType.from_str(state[0])
        self.current_price = Price.from_str(state[1])
        self.current_bar_type = BarType.from_str(state[2])
        self.forward_price = Price.from_str(state[3]) if state[3] is not None else None
        self.forward_bar_type = BarType.from_str(state[4])
        self.carry_price = Price.from_str(state[5]) if state[5] is not None else None
        self.carry_bar_type = BarType.from_str(state[6])
        self._ts_event = state[7]
        self._ts_init = state[8]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MultiplePrice):
            return False
        return (
            self.bar_type == other.bar_type
            and self.current_price == other.current_price
            and self.current_bar_type == other.current_bar_type
            and self.forward_price == other.forward_price
            and self.forward_bar_type == other.forward_bar_type
            and self.carry_price == other.carry_price
            and self.carry_bar_type == other.carry_bar_type
            and self._ts_event == other.ts_event
            and self._ts_init == other.ts_init
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"bar_type={self.bar_type}, "
            f"current_price={self.current_price}, "
            f"current_bar_type={self.current_bar_type}, "
            f"forward_price={self.forward_price}, "
            f"forward_bar_type={self.forward_bar_type}, "
            f"carry_price={self.carry_price}, "
            f"carry_bar_type={self.carry_bar_type}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )