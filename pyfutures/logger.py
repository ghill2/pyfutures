from __future__ import annotations
import datetime
import pandas as pd
import sys
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.common.enums import LogColor
import traceback

LOG_COLOR_TO_COLOR = {
    LogColor.NORMAL: "",
    LogColor.GREEN: "\x1b[92m",
    LogColor.BLUE: "\x1b[94m",
    LogColor.MAGENTA: "\x1b[35m",
    LogColor.CYAN: "\x1b[36m",
    LogColor.YELLOW: "\x1b[1;33m",
    LogColor.RED: "\x1b[1;31m",
}
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
            trading_class=getattr(LoggerAttributes, "trading_class", "N/A"),
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
        msg = self._format_line(message=message, level="DBG", color=color)
        print(msg)

    def info(
        self,
        message: str,
        color: LogColor = LogColor.NORMAL,
    ):
        msg = self._format_line(message=message, level="INF", color=color)
        print(msg)

    def warning(
        self,
        message,
        color: LogColor = LogColor.YELLOW,
    ):
        msg = self._format_line(message=message, level="WRN", color=color)
        print(msg)

    def error(
        self,
        message: str,
        color: LogColor = LogColor.RED,
    ):
        msg = self._format_line(message=message, level="ERR", color=color)
        print(msg)

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
    
    def _format_line(
        self,
        message: str,
        level: str,
        color: LogColor,
    ) -> str:
        """
        pub fn get_colored(&mut self) -> &str {
            self.colored.get_or_insert_with(|| {
                format!(
                    "\x1b[1m{}\x1b[0m {}[{}] {}.{}: {}\x1b[0m\n",
                    self.timestamp,
                    &self.line.color.to_string(),
                    self.line.level,
                    self.trader_id,
                    &self.line.component,
                    &self.line.message
                )
            })
        }
        """
        
        t = self._unix_nano_to_iso8601(self._timestamp_ns)
        c = LOG_COLOR_TO_COLOR[color]
        l = level
        id = "TRADER-001"
        comp = "TRADER-001"
        msg = message
        return f"\x1b[1m{t}\x1b[0m {c}[{l}] {id}.{comp}: {msg}\x1b[0m"  # \n
        
        # return f"{self.timestamp} WARNING {self._trading_class} {message}"
        
    @staticmethod
    def _unix_nano_to_iso8601(nanoseconds):
        # Convert nanoseconds to seconds
        seconds = nanoseconds / 1e9

        # Create a datetime object from the seconds
        dt = datetime.datetime.utcfromtimestamp(seconds)

        # Format the datetime object as an ISO 8601 string
        iso8601_str = dt.isoformat()

        return iso8601_str