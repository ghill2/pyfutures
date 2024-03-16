from __future__ import annotations

from pytower import PACKAGE_ROOT

import datetime
import logging
import re
import sys
import traceback
from pathlib import Path
import time
import inspect
import os


NORMAL = 0
GREEN = 1
BLUE = 2
MAGENTA = 3
CYAN = 4
YELLOW = 5
RED = 6

LOG_COLOR_TO_COLOR = {
    NORMAL: "",
    GREEN: "\x1b[92m",
    BLUE: "\x1b[94m",
    MAGENTA: "\x1b[35m",
    CYAN: "\x1b[36m",
    YELLOW: "\x1b[1;33m",
    RED: "\x1b[1;31m",
}

LEVEL_TO_STR = {
    logging.INFO: "INF",
    logging.DEBUG: "DBG",
    logging.WARNING: "WRN",
    logging.ERROR: "ERR",
    logging.CRITICAL: "CRT",
}


def init_logging(log_level: int = logging.DEBUG):
    # initializes all loggers with specified custom format and log level
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # Example log format
    formatter = logging.Formatter(log_format)
    handler = logging.StreamHandler()  # Output to console
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(log_level)


class LoggerAttributes:
    id: str = ""
    level: int = logging.DEBUG
    path: Path | None = None


class StripANSIFileHandler(logging.FileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def emit(self, record):
        record.msg = self._strip_ansi(record.msg)
        super().emit(record)

    @staticmethod
    def _strip_ansi(text):
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        return ansi_escape.sub("", text)


class LoggerAdapter:
    _timestamp_ns: int = 0

    def __init__(
        self,
        name: str,
        id: str = "N/A",
        level: int = logging.DEBUG,
        path: Path | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.level = level
        self.path = path

        self.logger = logging.Logger(name=name)
        # super().__init__(name=name)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self.level)
        handler.setFormatter(
            logging.Formatter("%(message)s"),
        )
        self.logger.addHandler(handler)

        if self.path is not None:
            handler = StripANSIFileHandler(self.path)
            handler.setLevel(self.level)
            handler.setFormatter(
                logging.Formatter("%(message)s"),
            )
            self.logger.addHandler(handler)

    # @classmethod
    # def set_timestamp_ns(cls, timestamp_ns: int) -> None:
    #     cls._timestamp_ns = timestamp_ns

    @classmethod
    def from_name(cls, name: str) -> LoggerAdapter:
        return cls(
            name=name,
            id=getattr(LoggerAttributes, "id"),
            level=getattr(LoggerAttributes, "level"),
            path=getattr(LoggerAttributes, "path"),
        )

    def debug(
        self,
        message: str,
        color: int = NORMAL,
    ):
        message = self._format_line(message=message, level=logging.DEBUG, color=color)
        self.logger.debug(message)

    def info(
        self,
        message: str,
        color: int = NORMAL,
    ):
        message: str = self._format_line(message=message, level=logging.INFO, color=color)
        self.logger.info(message)

    def warning(
        self,
        message,
        color: int = YELLOW,
    ):
        message: str = self._format_line(message=message, level=logging.WARNING, color=color)
        self.logger.warning(message)

    def error(
        self,
        message: str,
        color: int = RED,
    ):
        message: str = self._format_line(message=message, level=logging.ERROR, color=color)
        self.logger.error(message)

    def exception(
        self,
        message: str,
        ex,
    ):
        ex_string = f"{type(ex).__name__}({ex})"
        ex_type, ex_value, ex_traceback = sys.exc_info()
        stack_trace = traceback.format_exception(ex_type, ex_value, ex_traceback)

        stack_trace_lines = ""
        for line in stack_trace[: len(stack_trace) - 1]:
            stack_trace_lines += line

        self.error(f"{message}\n{ex_string}\n{stack_trace_lines}")

    @staticmethod
    def _format_path(path):
        """
        iterate backwards on the filepath until a dirname is matched
        """
        components = path.split(os.sep)  # Split path into components based on separator
        for i in range(len(components) - 1, -1, -1):  # Iterate backwards
            if components[i] in ("nautilus_trader", "pytower", "pyfutures"):
                return os.sep.join(components[i:])
        return path  # Return original path if not found

    def _format_line(
        self,
        message: str,
        level: int,
        color: int | None,
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
        t = self._unix_nanos_to_iso8601(time.time_ns())
        l = LEVEL_TO_STR[level]

        caller_frame = inspect.currentframe().f_back.f_back
        filepath = caller_frame.f_code.co_filename
        filepath = self._format_path(filepath)
        line_number = caller_frame.f_lineno
        func_name = caller_frame.f_code.co_name

        msg = message

        if color is None:
            return f"{t} [{l}] {filepath}::{func_name}::{line_number} [{l}] {self.name} {self.id}: {msg}"
        else:
            c = LOG_COLOR_TO_COLOR[color]
            return f"\x1b[1m{t}\x1b[0m {c} [{l}] {filepath}::{func_name}::{line_number} {self.name} {self.id}: {msg}\x1b[0m"

        # return f"{self.timestamp} WARNING {self._trading_class} {message}"

    @staticmethod
    def _unix_nanos_to_iso8601(nanoseconds):
        # Convert nanoseconds to seconds
        seconds = nanoseconds / 1e9

        # Create a datetime object from the seconds
        dt = datetime.datetime.utcfromtimestamp(seconds)

        # Format the datetime object as an ISO 8601 string
        iso8601_str = dt.isoformat()

        return iso8601_str


# class DynamicAttributeFormatter(logging.Formatter):

#     timestamp_ns = 0
#     log_: str

#     def __init__(
#         self,
#         trading_class: str,
#     ):
#         super().__init__(
#             fmt=f"\x1b[1m{t}\x1b[0m {c}[{l}] {id}.{comp}: {msg}\x1b[0m"
#         )
#         self.trading_class = trading_class

#     def format(self, record):
#         setattr(record, self.custom_attribute, self.get_custom_attribute_value())
#         return super().format(record)

#     def _format_line(
#         self,
#         message: str,
#         level: str,
#         color: LogColor,
#     ) -> str:
#         """
#         pub fn get_colored(&mut self) -> &str {
#             self.colored.get_or_insert_with(|| {
#                 format!(
#                     "\x1b[1m{}\x1b[0m {}[{}] {}.{}: {}\x1b[0m\n",
#                     self.timestamp,
#                     &self.line.color.to_string(),
#                     self.line.level,
#                     self.trader_id,
#                     &self.line.component,
#                     &self.line.message
#                 )
#             })
#         }
#         """

#         t = self._unix_nano_to_iso8601(self._timestamp_ns)
#         c = LOG_COLOR_TO_COLOR[color]
#         l = level
#         id = "TRADER-001"
#         comp = "TRADER-001"
#         msg = message
#         return f"\x1b[1m{t}\x1b[0m {c}[{l}] {id}.{comp}: {msg}\x1b[0m"  # \n

#         # return f"{self.timestamp} WARNING {self._trading_class} {message}"

#     @staticmethod
#     def _unix_nano_to_iso8601(nanoseconds):
#         # Convert nanoseconds to seconds
#         seconds = nanoseconds / 1e9

#         # Create a datetime object from the seconds
#         dt = datetime.datetime.utcfromtimestamp(seconds)

#         # Format the datetime object as an ISO 8601 string
#         iso8601_str = dt.isoformat()

#         return iso8601_str
