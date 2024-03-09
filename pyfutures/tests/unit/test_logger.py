import logging
import sys
import tempfile
from pathlib import Path

from pyfutures.logger import LOG_COLOR_TO_COLOR
from pyfutures.logger import NORMAL
from pyfutures.logger import RED
from pyfutures.logger import YELLOW
from pyfutures.logger import LoggerAdapter
from pyfutures.logger import LoggerAttributes
from pyfutures.tests.unit.client.stubs import ClientStubs


class TestLogger:
    def setup_method(self):
        self.path = Path(tempfile.mkdtemp()) / "test.log"
        self.log: LoggerAdapter = ClientStubs.logger_adapter(
            level=logging.DEBUG,
            path=self.path,
        )
        self.message = "test log message"

    def teardown_method(self):
        self.path.unlink()

    def test_logger_init_(self):
        log = LoggerAdapter(
            name="test_name",
            id="test_id",
            level=logging.DEBUG,
            path=Path("test.log"),
        )
        assert log.name == "test_name"
        assert log.id == "test_id"
        assert log.level == logging.DEBUG
        assert log.path == Path("test.log")
        assert isinstance(log.logger, logging.Logger)

    def test_from_name(self):
        LoggerAttributes.name = "test_name"
        LoggerAttributes.id = "test_id"
        LoggerAttributes.level = logging.DEBUG
        LoggerAttributes.path = Path("test.log")

        log = LoggerAdapter.from_name(
            name="test_name",
        )
        assert log.name == "test_name"
        assert log.id == "test_id"
        assert log.level == logging.DEBUG
        assert log.path == Path("test.log")

    def test_console_info_writes_expected(self):
        # Arrange & Act
        self.log.info(self.message)

        # Assert
        logged: str = sys.stdout.getvalue().splitlines()[-1]
        self._assert_colored_logged_message(logged)
        assert LOG_COLOR_TO_COLOR[NORMAL] in logged

    def test_console_debug_writes_expected(self):
        # Arrange & Act
        self.log.debug(self.message)

        # Assert
        logged: str = sys.stdout.getvalue().splitlines()[-1]
        self._assert_colored_logged_message(logged)
        assert LOG_COLOR_TO_COLOR[NORMAL] in logged

    def test_console_warning_writes_expected(self):
        # Arrange & Act
        self.log.warning(self.message)

        # Assert
        logged: str = sys.stdout.getvalue().splitlines()[-1]
        self._assert_colored_logged_message(logged)
        assert LOG_COLOR_TO_COLOR[YELLOW] in logged

    def test_console_error_writes_expected(self):
        # Arrange & Act
        self.log.error(self.message)

        # Assert
        logged: str = sys.stdout.getvalue().splitlines()[-1]
        self._assert_colored_logged_message(logged)
        assert LOG_COLOR_TO_COLOR[RED] in logged

    def test_console_exception_writes_expected(self):
        # Arrange & Act
        ex = ValueError("test")
        self.log.exception(self.message, ex=ex)

        logged: str = sys.stdout.getvalue()
        assert LOG_COLOR_TO_COLOR[RED] in logged
        self._assert_colored_logged_message(logged.rstrip("\n"))
        assert type(ex).__name__ in logged
        assert str(ex) in logged

    def test_console_info_level_writes_expected(self):
        # Arrange
        log: LoggerAdapter = ClientStubs.logger_adapter(level=logging.INFO)
        assert log.level == logging.INFO

        # Act
        log.warning(self.message)
        log.error(self.message)
        log.info(self.message)
        log.debug(self.message)

        # Assert
        lines: list[str] = sys.stdout.getvalue().splitlines()
        assert len(lines) == 3

    def test_console_debug_level_writes_expected(self):
        # Arrange
        log: LoggerAdapter = ClientStubs.logger_adapter(level=logging.DEBUG)
        assert log.level == logging.DEBUG

        # Act
        log.warning(self.message)
        log.error(self.message)
        log.info(self.message)
        log.debug(self.message)

        # Assert
        lines: list[str] = sys.stdout.getvalue().splitlines()
        assert len(lines) == 4

    def test_console_warning_level_writes_expected(self):
        # Arrange
        log: LoggerAdapter = ClientStubs.logger_adapter(level=logging.WARNING)
        assert log.level == logging.WARNING

        # Act
        log.warning(self.message)
        log.error(self.message)
        log.info(self.message)
        log.debug(self.message)

        # Assert
        lines: list[str] = sys.stdout.getvalue().splitlines()
        assert len(lines) == 2

    def test_console_error_level_writes_expected(self):
        # Arrange
        log: LoggerAdapter = ClientStubs.logger_adapter(level=logging.ERROR)
        assert log.level == logging.ERROR

        # Act
        log.warning(self.message)
        log.error(self.message)
        log.info(self.message)
        log.debug(self.message)

        # Assert
        lines: list[str] = sys.stdout.getvalue().splitlines()
        assert len(lines) == 1

    def test_file_info_writes_expected(self):
        # Arrange & Act
        self.log.info(self.message)

        # Assert
        logged: str = self.log.path.read_text().splitlines()[-1]
        self._assert_uncolored_logged_message(logged)

    def test_file_debug_writes_expected(self):
        # Arrange & Act
        self.log.debug(self.message)

        # Assert
        logged: str = self.log.path.read_text().splitlines()[-1]
        self._assert_uncolored_logged_message(logged)

    def test_file_warning_writes_expected(self):
        # Arrange & Act
        self.log.warning(self.message)

        # Assert
        logged: str = self.log.path.read_text().splitlines()[-1]
        self._assert_uncolored_logged_message(logged)

    def test_file_error_writes_expected(self):
        # Arrange & Act
        self.log.error(self.message)

        # Assert
        logged: str = self.log.path.read_text().splitlines()[-1]
        self._assert_uncolored_logged_message(logged)

    def test_exception_error_writes_expected(self):
        # Arrange & Act
        ex = ValueError("test")
        self.log.exception(self.message, ex=ex)

        # Assert
        logged: str = self.log.path.read_text()
        self._assert_uncolored_logged_message(logged.rstrip("\n"))
        assert type(ex).__name__ in logged
        assert str(ex) in logged

    def _assert_uncolored_logged_message(self, logged: str) -> bool:
        assert self.log.name in logged
        assert self.log.id in logged
        assert LoggerAdapter._unix_nanos_to_iso8601(self.log._timestamp_ns) in logged
        assert self.message in logged

    def _assert_colored_logged_message(self, logged: str) -> bool:
        self._assert_uncolored_logged_message(logged)
        assert logged.endswith("\x1b[0m")
