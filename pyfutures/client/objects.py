from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.contract import Contract as IBContract
from ibapi.execution import Execution as IBExecution
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState


@dataclass
class IBOpenOrderEvent:
    contract: IBContract
    order: IBOrder
    orderState: IBOrderState


@dataclass
class IBPortfolioEvent:
    conId: int
    position: Decimal
    marketPrice: float
    marketValue: float
    averageCost: float
    unrealizedPNL: float
    realizedPNL: float
    accountName: str


@dataclass
class IBPositionEvent:
    account: str
    conId: int
    quantity: Decimal
    avgCost: float


@dataclass
class IBExecutionEvent:
    timestamp: pd.Timestamp
    reqId: int
    contract: IBContract
    execution: IBExecution
    commissionReport: IBCommissionReport


@dataclass
class IBErrorEvent:
    reqId: int
    errorCode: int
    errorString: str
    advancedOrderRejectJson: str


@dataclass
class IBOrderStatusEvent:
    orderId: int
    status: str
    filled: Decimal
    remaining: Decimal
    avgFillPrice: float
    permId: int
    parentId: int
    lastFillPrice: float
    clientId: int
    whyHeld: str
    mktCapPrice: float


@dataclass
class ClientRequest(asyncio.Future):
    id: int | str
    data: list | dict | None = None
    timeout_seconds: int = 5

    def __post_init__(self):
        super().__init__()


@dataclass
class ClientSubscription:
    id: int
    subscribe: Callable
    cancel: Callable
    callback: Callable


class ClientException(Exception):
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(code, message)  # Pass code and message as separate arguments

    def __str__(self):
        return f"{self.__class__.__name__}: {self.code}: {self.message}"

    def __getstate__(self):
        return {
            "code": self.code,
            "message": self.message,
        }

    def __setstate__(self, state):
        self.code = state["code"]
        self.message = state["message"]

    def to_dict(self):
        return self.__getstate__()

    @classmethod
    def from_dict(cls, obj: dict) -> ClientException:
        return cls(code=obj["code"], message=obj["message"])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ClientException):
            return False
        return self.code == other.code and self.message == other.message


class ClientDisconnected(Exception):
    def __init__(self, message: str = None):
        super().__init__(message)


# class TimeoutError(asyncio.TimeoutError):
#     """asyncio.TimeoutError that stores the timeout_seconds for use in Historic Client"""
#
#     def __init__(self, timeout_seconds: int):
#         self.timeout_seconds = timeout_seconds
#
#     def __str__(self):
#         return f"{self.__class__.__name__}: timeout_seconds={self.timeout_seconds}"
#
#     """The operation exceeded the given deadline."""

# @dataclass
# class IBBar:
#     name: str
#     time: int
#     open: float
#     high: float
#     low: float
#     close: float
#     volume: Decimal
#     wap: Decimal
#     count: int
#
#     @staticmethod
#     def to_dict(obj: IBBar) -> dict:
#         {
#             "date": [parse_datetime(bar.date) for bar in bars],
#             "open": [bar.open for bar in bars],
#             "high": [bar.high for bar in bars],
#             "low": [bar.low for bar in bars],
#             "close": [bar.close for bar in bars],
#             "volume": [float(bar.volume) for bar in bars],
#             "wap": [float(bar.wap) for bar in bars],
#             "barCount": [bar.barCount for bar in bars],
#         }

# @dataclass
# class IBQuoteTick:
#     name: str
#     time: pd.Timestamp
#     bid_price: float
#     ask_price: float
#     bid_size: Decimal
#     ask_size: Decimal
#

# @dataclass
# class IBTradeTick:
#     name: str
#     time: int
#     price: float
#     size: Decimal
#     exchange: str
#     conditions: str
#


# @dataclass
# class IBBar:
#     timestamp: pd.Timestamp
#     open: float
#     high: float
#     low: float
#     close: float
#     volume: Decimal
#     wap: int
#     barCount: int

#     @staticmethod
#     def from_bar_data(obj: BarData) -> IBBar:
#         pass

# @dataclass
# class QuoteTick:
#     pass
