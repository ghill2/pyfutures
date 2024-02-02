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
        forward_month: ContractMonth,
        current_price: Price,
        current_month: ContractMonth,
        carry_price: Price | None,
        carry_month: ContractMonth,
        ts_event: int,
        ts_init: int,
    ):
        super().__init__()

        self.bar_type = bar_type
        self.instrument_id = bar_type.instrument_id
        self.current_price = current_price
        self.current_month = current_month
        self.forward_price = forward_price
        self.forward_month = forward_month
        self.carry_price = carry_price
        self.carry_month = carry_month
        self._ts_event = ts_event
        self._ts_init = ts_init

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init
    
        
    @property
    def current_bar_type(self):
        return BarType(
            instrument_id=self._chain.current_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
        
    @property
    def forward_bar_type(self):
        return BarType(
            instrument_id=self._chain.forward_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
        
    @property
    def carry_bar_type(self):
        return BarType(
            instrument_id=self._chain.carry_contract.id,
            bar_spec=self._bar_spec,
            aggregation_source=self._aggregation_source,
        )
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MultiplePrice):
            return False
        return (
            self.bar_type == other.bar_type
            and self.current_price == other.current_price
            and self.current_month == other.current_month
            and self.forward_price == other.forward_price
            and self.forward_month == other.forward_month
            and self.carry_price == other.carry_price
            and self.carry_month == other.carry_month
            and self._ts_event == other.ts_event
            and self._ts_init == other.ts_init
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"bar_type={self.bar_type}, "
            f"current_price={self.current_price}, "
            f"current_month={self.current_month}, "
            f"forward_price={self.forward_price}, "
            f"forward_month={self.forward_month}, "
            f"carry_price={self.carry_price}, "
            f"carry_month={self.carry_month}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )

    @staticmethod
    def schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("bar_type", pa.dictionary(pa.int16(), pa.string())),
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
    def to_dict(obj: MultiplePrice) -> dict:
        return {
            "bar_type": str(obj.bar_type),
            "current_price": str(obj.current_price),
            "current_month": obj.current_month.value,
            "forward_price": str(obj.forward_price) if obj.forward_price is not None else None,
            "forward_month": obj.forward_month.value,
            "carry_price": str(obj.carry_price) if obj.carry_price is not None else None,
            "carry_month": obj.carry_month.value,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }

    @staticmethod
    def from_dict(values: dict) -> MultiplePrice:
        PyCondition.not_none(values, "values")
        return MultiplePrice(
            bar_type=BarType.from_str(values["bar_type"]),
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
            str(self.bar_type),
            str(self.current_price),
            self.current_month.value,
            str(self.forward_price),
            self.forward_month.value,
            str(self.carry_price),
            self.carry_month.value,
            self.ts_event,
            self.ts_init,
        )

    def __setstate__(self, state):
        self.bar_type = BarType.from_str(state[0])
        self.current_price = Price.from_str(state[1])
        self.current_month = ContractMonth(state[2])
        self.forward_price = Price.from_str(state[3])
        self.forward_month = ContractMonth(state[4])
        self.carry_price = Price.from_str(state[5])
        self.carry_month = ContractMonth(state[6])
        self._ts_event = state[7]
        self._ts_init = state[8]
