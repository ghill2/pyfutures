from pyfutures.client.client import InteractiveBrokersClient
import inspect
import sys
from pyfutures import PACKAGE_ROOT
import asyncio
import subprocess
import hashlib

from pathlib import Path

from pyfutures.logger import LoggerAdapter
from pyfutures.logger import init_logging
import logging


class MockServerSubproc:
    def __init__(self, loop):
        self._loop = loop
        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self._read_stdout_task: asyncio.Task | None = None
        self._read_stderr_task: asyncio.Task | None = None
        self._proc: asyncio.Process | None = None

    async def start(self):
        path = PACKAGE_ROOT / "tests" / "bytestring" / "mock_server.py"
        self._proc = await asyncio.create_subprocess_exec(
            sys.executable,
            path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # self.on_subproc_exit_task = self._loop.create_task(
        #     self._on_subproc_exit_task(), name="mock_server_subproc on_process_exited"
        # )
        self._read_stdout_task = self._loop.create_task(
            self.read_stdout_task(), name="mock_server_subproc read_stdout_task"
        )
        self._read_stderr_task = self._loop.create_task(
            self.read_stderr_task(), name="mock_server_subproc read_stderr_task"
        )

        return self._proc

    # Tasks
    async def _on_subproc_exit_task(self):
        print("on subproc exit task")

        # not using await self._proc.wait() intentionally
        # as it does not let other tasks run
        #
        # if the mock_server exits while the main process is running
        # exit the main process
        # while self._proc.transport._returncode is None:
        # await asyncio.sleep(0)
        await self._proc.wait()
        self._log.debug("mock server exited...")
        exit()

    async def read_stdout_task(self):
        try:
            while True:
                await asyncio.sleep(0)
                # do not use stdout.readline()
                buf = await self._proc.stdout.read(256)

                if not buf:
                    continue
                self._log.debug(buf.decode("utf-8"), color=4)
        except Exception as e:
            self._log.exception("mock_server_subproc read_stderr exception", e)

    async def read_stderr_task(self):
        try:
            while True:
                await asyncio.sleep(0)
                buf = await self._proc.stderr.read(256)
                if not buf:
                    continue

                self._log.error(buf.decode("utf-8"), color=4)
        except Exception as e:
            self._log.exception("mock_server_subproc read_stderr exception", e)

    async def write_stdin(self, msg: str):
        self._proc.stdin.write(msg.encode("utf-8"))
        await self._proc.stdin.drain()


class SingletonMeta(type):
    """
    Metaclass that creates a Singleton class.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Overrides the default __call__ method to ensure only one instance exists.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class BytestringClientStubs(metaclass=SingletonMeta):
    _mock_server_subproc: MockServerSubproc | None = None
    _client: InteractiveBrokersClient | None = None

    def __init__(self, mode, loop):
        self._mode = mode
        self._loop = loop

    async def client(self, *args, **kwargs):
        if self._client is None:
            self._client = InteractiveBrokersClient(port=8890, *args, **kwargs)

        self._loop.set_debug(True)
        # names = logging.Logger.manager.loggerDict
        init_logging()
        # logging.getLogger("asyncio").setLevel(logging.DEBUG)
        # for name in names:
        # print(name)
        # if "ibapi" in name:
        # logging.getLogger(name).setLevel(api_log_level)

        caller_frame = sys._getframe(1)

        # ensure caller function name begins with test_
        caller_fn_name = caller_frame.f_code.co_name
        assert caller_fn_name.startswith("test_")

        # hash parent function source code
        source = inspect.getsource(caller_frame)
        clean_fn_string = "\n".join(
            line.strip() for line in source.splitlines() if not line.startswith("#")
        )
        # Hash the cleaned string using a cryptographic hash function (e.g., SHA-256)
        fn_hash = hashlib.sha256(clean_fn_string.encode("utf-8")).hexdigest()

        # Get the filepath of the bytestrings for the current test
        caller_filename = Path(inspect.getfile(caller_frame)).stem
        parent = PACKAGE_ROOT / "tests" / "bytestring" / "txt"
        bytestrings_path = parent / f"{caller_filename}={caller_fn_name}={fn_hash}.py"

        if self._mode == "unit":
            if self._mock_server_subproc is None:
                self._mock_server_subproc = MockServerSubproc(loop=self._loop)
                await self._mock_server_subproc.start()
                await asyncio.sleep(3)
            # await self._mock_server_subproc.write_stdin(str(bytestrings_path))

        if self._mode == "demo":
            self._client.conn.protocol.export_bytestrings(path=bytestrings_path)

        return self._client

    # @staticmethod
    # def comm_client():
    #     global COMM_CLIENT
    #     if COMM_CLIENT is None:
    #         COMM_CLIENT = BytestringCommClient()
    #     return COMM_CLIENT
