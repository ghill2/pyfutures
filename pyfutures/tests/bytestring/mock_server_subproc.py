import asyncio
import subprocess
import sys

from pyfutures import PACKAGE_ROOT
from pyfutures.logger import LoggerAdapter


class MockServerSubproc:
    def __init__(self, loop):
        self._loop = loop
        self._log = LoggerAdapter.from_name(
            name=type(self).__name__, prefix=lambda _: ""
        )
        self._read_stdout_task: asyncio.Task | None = None
        self._read_stderr_task: asyncio.Task | None = None
        self._proc: asyncio.Process | None = None
        self._server_ready_waiter = None
        self._command_waiter = None

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
        # self._server_ready_waiter = self._loop.create_future()
        # self._log.debug("Waiting for mock_server to ready...")
        # try:
        #     await self._server_ready_waiter
        # finally:
        #     self._server_ready_waiter = None

        return self._proc

    async def perform_command(self, cmd: str, value: str):
        self._proc.stdin.write(cmd.encode("ascii") + b"\x00" + value.encode("ascii"))
        await self._proc.stdin.drain()

        self._command_waiter = self._loop.create_future()
        self._log.debug(f"Performing subproc command: {cmd}...")
        try:
            await self._command_waiter
        finally:
            self._command_waiter = None

    # Tasks
    async def _on_subproc_exit_task(self):
        try:
            await self._proc.wait()
            self._log.error("mock_server exited - CANCELLING ALL TASKS...")
            for task in asyncio.all_tasks():
                task.cancel()
        except Exception as e:
            self._log.exception("mock_server_subproc on_subproc_exit_task exception", e)

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
                    self._command_waiter.set_result(None)
                    self._log.debug("mock_server - bytestrings ready...")

                if buf == b"COMMAND SUCCESS":
                    # self._log.debug("Bytestrings ready...")
                    self._command_waiter.set_result(None)

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
