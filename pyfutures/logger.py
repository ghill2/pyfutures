from __future__ import annotations
import pandas as pd
import sys
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.common.enums import LogColor
import traceback

class LoggerAttributes:
    pass

class LoggerAdapter:
    
    _timestamp_ns = 0
    
    def __init__(
        self,
        name: str,
        trading_class: str,
    ) -> None:
        self._name = name
        self._trading_class = trading_class
        
    @classmethod
    def set_time(cls, time_ns: int):
        cls._timestamp_ns = time_ns
        
    @classmethod
    def from_name(cls, name: str) -> LoggerAdapter:
        return cls(
            name=name,
            trading_class=LoggerAttributes.trading_class,
        )
        
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def timestamp(self) -> pd.Timestamp:
        return unix_nanos_to_dt(self._timestamp_ns)
    
    def debug(
        self,
        message: str,
        color: LogColor = LogColor.NORMAL,
    ):
        print(f"{self.timestamp} DEBUG {self._trading_class} {message}")

    def info(
        self,
        message: str,
        color: LogColor = LogColor.NORMAL,
    ):
        print(f"{self.timestamp} INFO {self._trading_class} {message}")

    def warning(
        self,
        message,
        color: LogColor = LogColor.YELLOW,
    ):
        print(f"{self.timestamp} WARNING {self._trading_class} {message}")

    def error(
        self,
        message: str,
        color: LogColor = LogColor.RED,
    ):
        print(f"{self.timestamp} ERROR {self._trading_class} {message}")

    def exception(
        self,
        message: str,
        ex,
    ):
        Condition.not_none(ex, "ex")

        ex_string = f"{type(ex).__name__}({ex})"
        ex_type, ex_value, ex_traceback = sys.exc_info()
        stack_trace = traceback.format_exception(ex_type, ex_value, ex_traceback)

        stack_trace_lines = ""
        for line in stack_trace[:len(stack_trace) - 1]:
            stack_trace_lines += line

        self.error(f"{message}\n{ex_string}\n{stack_trace_lines}")