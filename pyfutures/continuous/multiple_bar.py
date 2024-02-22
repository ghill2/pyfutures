from __future__ import annotations

import pyarrow as pa

from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType


class MultipleBar(Data):
    def __init__(
        self,
        bar_type: BarType,
        current_bar: Bar,
        forward_bar: Bar,
        carry_bar: Bar | None,
        ts_event: int,
        ts_init: int,
    ):
        self.bar_type = bar_type
        self.instrument_id = bar_type.instrument_id
        self.current_bar = current_bar
        self.forward_bar = forward_bar
        self.carry_bar = carry_bar
        self._ts_event = ts_event
        self._ts_init = ts_init

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init

    @property
    def current_month(self) -> ContractMonth:
        return ContractMonth(self.current_bar.bar_type.instrument_id.symbol.value.split("=")[-1])

    @property
    def forward_month(self) -> ContractMonth:
        return ContractMonth(self.forward_bar.bar_type.instrument_id.symbol.value.split("=")[-1])

    @property
    def carry_month(self) -> ContractMonth | None:
        if self.carry_bar is None:
            return None
        return ContractMonth(self.carry_bar.bar_type.instrument_id.symbol.value.split("=")[-1])

    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("current_bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("current_open", pa.string()),
                pa.field("current_high", pa.string()),
                pa.field("current_low", pa.string()),
                pa.field("current_close", pa.string()),
                pa.field("current_volume", pa.string()),
                pa.field("current_ts_event", pa.uint64()),
                pa.field("current_ts_init", pa.uint64()),
                pa.field("forward_bar_type", pa.dictionary(pa.int16(), pa.string())),
                pa.field("forward_open", pa.string()),
                pa.field("forward_high", pa.string()),
                pa.field("forward_low", pa.string()),
                pa.field("forward_close", pa.string()),
                pa.field("forward_volume", pa.string()),
                pa.field("forward_ts_event", pa.uint64()),
                pa.field("forward_ts_init", pa.uint64()),
                pa.field("carry_bar_type", pa.dictionary(pa.int16(), pa.string()), nullable=True),
                pa.field("carry_open", pa.string(), nullable=True),
                pa.field("carry_high", pa.string(), nullable=True),
                pa.field("carry_low", pa.string(), nullable=True),
                pa.field("carry_close", pa.string(), nullable=True),
                pa.field("carry_volume", pa.string(), nullable=True),
                pa.field("carry_ts_event", pa.uint64(), nullable=True),
                pa.field("carry_ts_init", pa.uint64(), nullable=True),
                pa.field("ts_event", pa.uint64(), nullable=True),
                pa.field("ts_init", pa.uint64(), nullable=True),
            ],
        )

    @staticmethod
    def to_dict(obj: MultipleBar) -> dict:
        return {
            "bar_type": str(obj.bar_type),
            "current_bar_type": str(obj.current_bar.bar_type),
            "current_open": str(obj.current_bar.open),
            "current_high": str(obj.current_bar.high),
            "current_low": str(obj.current_bar.low),
            "current_close": str(obj.current_bar.close),
            "current_volume": str(obj.current_bar.volume),
            "current_ts_event": obj.current_bar.ts_event,
            "current_ts_init": obj.current_bar.ts_init,
            "forward_bar_type": str(obj.forward_bar.bar_type),
            "forward_open": str(obj.forward_bar.open),
            "forward_high": str(obj.forward_bar.high),
            "forward_low": str(obj.forward_bar.low),
            "forward_close": str(obj.forward_bar.close),
            "forward_volume": str(obj.forward_bar.volume),
            "forward_ts_event": obj.forward_bar.ts_event,
            "forward_ts_init": obj.forward_bar.ts_init,
            "carry_bar_type": str(obj.carry_bar.bar_type) if obj.carry_bar is not None else None,
            "carry_open": str(obj.carry_bar.open) if obj.carry_bar is not None else None,
            "carry_high": str(obj.carry_bar.high) if obj.carry_bar is not None else None,
            "carry_low": str(obj.carry_bar.low) if obj.carry_bar is not None else None,
            "carry_close": str(obj.carry_bar.close) if obj.carry_bar is not None else None,
            "carry_volume": str(obj.carry_bar.volume) if obj.carry_bar is not None else None,
            "carry_ts_event": obj.carry_bar.ts_event if obj.carry_bar is not None else None,
            "carry_ts_init": obj.carry_bar.ts_init if obj.carry_bar is not None else None,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }

    @staticmethod
    def from_dict(values: dict) -> MultipleBar:
        PyCondition.not_none(values, "values")
        return MultipleBar(
            bar_type=BarType.from_str(values["bar_type"]),
            current_bar=Bar(
                bar_type=BarType.from_str(values["current_bar_type"]),
                open=Price.from_str(values["current_open"]),
                high=Price.from_str(values["current_high"]),
                low=Price.from_str(values["current_low"]),
                close=Price.from_str(values["current_close"]),
                volume=Quantity.from_str(values["current_volume"]),
                ts_event=values["current_ts_event"],
                ts_init=values["current_ts_init"],
            ),
            forward_bar=Bar(
                bar_type=BarType.from_str(values["forward_bar_type"]),
                open=Price.from_str(values["forward_open"]),
                high=Price.from_str(values["forward_high"]),
                low=Price.from_str(values["forward_low"]),
                close=Price.from_str(values["forward_close"]),
                volume=Quantity.from_str(values["forward_volume"]),
                ts_event=values["forward_ts_event"],
                ts_init=values["forward_ts_init"],
            ),
            carry_bar=Bar(
                bar_type=BarType.from_str(values["carry_bar_type"]),
                open=Price.from_str(values["carry_open"]),
                high=Price.from_str(values["carry_high"]),
                low=Price.from_str(values["carry_low"]),
                close=Price.from_str(values["carry_close"]),
                volume=Quantity.from_str(values["carry_volume"]),
                ts_event=values["carry_ts_event"],
                ts_init=values["carry_ts_init"],
            )
            if values.get("carry_bar_type")
            else None,
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    def __getstate__(self) -> tuple:
        return (
            str(self.bar_type),
            self.current_bar,
            self.forward_bar,
            self.carry_bar,
            self._ts_init,
            self._ts_event,
        )

    def __setstate__(self, state):
        self.bar_type = BarType.from_str(state[0])
        self.current_bar = state[1]
        self.forward_bar = state[2]
        self.carry_bar = state[3]
        self._ts_event = state[4]
        self._ts_init = state[5]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MultipleBar):
            return False
        return (
            self.bar_type == other.bar_type
            and self.current_bar == other.current_bar
            and self.forward_bar == other.forward_bar
            and self.carry_bar == other.carry_bar
            and self._ts_event == other.ts_event
            and self._ts_init == other.ts_init
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"bar_type={self.bar_type}, "
            f"current_bar={self.current_bar}, "
            f"forward_bar={self.forward_bar}, "
            f"carry_bar={self.carry_bar}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )
