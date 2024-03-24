import asyncio
import pickle
import subprocess
import sys

from pyfutures import PACKAGE_ROOT
from pyfutures.logger import LoggerAdapter
import pytest


class MockServerSubproc:
    def __init__(self, loop):
        self._loop = loop
        self._log = LoggerAdapter.from_attrs(
            name=type(self).__name__, prefix=lambda _: ""
        )
        self._read_stdout_task: asyncio.Task | None = None
        self._read_stderr_task: asyncio.Task | None = None
        self._proc: asyncio.Process | None = None
        self._server_ready_waiter = None
        self._bytestrings_ready_waiter = None

    async def start(self):
        path = PACKAGE_ROOT / "tests" / "bytestring" / "mock_server.py"
        self._proc = await asyncio.create_subprocess_exec(
            sys.executable,
            path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.on_subproc_exit_task = self._loop.create_task(
            self._on_subproc_exit_task(), name="mock_server_subproc on_process_exited"
        )
        self._read_stdout_task = self._loop.create_task(
            self.read_stdout_task(), name="mock_server_subproc read_stdout_task"
        )
        self._read_stderr_task = self._loop.create_task(
            self.read_stderr_task(), name="mock_server_subproc read_stderr_task"
        )

        # wait until the server socket is ready
        self._server_ready_waiter = self._loop.create_future()
        self._log.debug("Waiting for mock_server to ready...")
        try:
            await self._server_ready_waiter
        finally:
            self._server_ready_waiter = None

        return self._proc

    async def load_bytestrings(self, path: str):
        self._proc.stdin.write(path.encode("utf-8"))
        await self._proc.stdin.drain()

        self._bytestrings_ready_waiter = self._loop.create_future()
        self._log.debug("Waiting for mock_server to ready...")
        try:
            await self._bytestrings_ready_waiter
        finally:
            self._bytestrings_ready_waiter = None

    async def cleanup(self):
        self.on_subproc_exit_task.cancel()
        self._read_stdout_task.cancel()
        self._read_stderr_task.cancel()
        self._loop.stop()

    # Tasks
    async def _on_subproc_exit_task(self):
        await self._proc.wait()
        self._log.debug("mock_server exited...")
        self.cleanup()
        # pytest.fail("Test failed due to condition in MyClass")

    async def read_stdout_task(self):
        try:
            while True:
                await asyncio.sleep(0)
                # do not use stdout.readline()
                buf = await self._proc.stdout.readline()
                buf = buf.rstrip(b"\n")
                if not buf:
                    continue

                if buf == b"MOCK_SERVER READY":
                    self._server_ready_waiter.set_result(None)
                    self._log.debug("mock_server ready...")

                if buf == b"BYTESTRINGS READY":
                    self._log.debug("Bytestrings ready...")
                    self._bytestrings_ready_waiter.set_result(None)

                self._log.debug(buf, color=4)
        except Exception as e:
            self._log.exception("mock_server_subproc read_stout_task exception", e)

    async def read_stderr_task(self):
        try:
            while True:
                await asyncio.sleep(0)
                buf = await self._proc.stderr.readline()
                buf = buf.rstrip(b"\n")
                if not buf:
                    continue

                self._log.error(buf, color=3)
        except Exception as e:
            self._log.exception("mock_server_subproc read_stderr_task exception", e)
