from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.tests.bytestring.unitize import BytestringCommClient
from pyfutures.tests.bytestring.unitize import BytestringCommClient
import inspect
import sys
from pyfutures import PACKAGE_ROOT
import subprocess
import asyncio

class MockServerSubproc:
    def __init__(self):
        self.poll_task: asyncio.Task | None = None
        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self.start()  # TODO: change from init when I change caching

    async def _poll_task():
        while True:
            await asyncio.sleep(0)

    def write(msg: str):
        self.proc.stdin.write(msg.encode("utf-8"))

    def start(self):
        filepath = PACKAGE_ROOT / "tests" / "bytestring" / "mock_server.py"
        self.proc = subprocess.Popen(
            [sys.executable] + list(filepath),
            stdin=subprocess.STDIN,
            stdout=subprocess.STDOUT,
            stderr=subprocess.STDERR  # Redirect stderr to stdout for simpler logging
        )
        for line in self.proc.stdout:
            self._log.debug(line.decode("utf-8").rstrip())
        for line in self.proc.stderr:
            self._log.error(line.decode("utf-8").rstrip())

        asyncio.create_task(self._poll_task(), name="poll")
            return proc


CLIENT = None
COMM_CLIENT = None
MOCK_SERVER_SUBPROCESS = None


class BytestringClientStubs:
    @staticmethod
    def _client(*args, **kwargs):
        global CLIENT
        if CLIENT is None:
            CLIENT = InteractiveBrokersClient(*args, **kwargs)
        return CLIENT

    # @staticmethod
    # def comm_client():
    #     global COMM_CLIENT
    #     if COMM_CLIENT is None:
    #         COMM_CLIENT = BytestringCommClient()
    #     return COMM_CLIENT

    @staticmethod
    def mock_server_subproc():
        global MOCK_SERVER_SUBPROCESS
        if MOCK_SERVER_SUBPROCESS is None:
            MOCK_SERVER_SUBPROCESS = BytestringCommClient()
        return MOCK_SERVER_SUBPROCESS

    @staticmethod
    def client(mode=None, *args, **kwargs):
        client = BytestringClientStubs._client(*args, **kwargs)

        if not mode:
            return client

        caller_frame = sys._getframe(1)

        # ensure caller function name begins with test_
        caller_fn_name = caller_frame.f_code.co_name
        assert caller_fn_name.startswith("test_")

        # hash parent function source code
        fn_hash = hash(inspect.getsource(caller_frame))

        # Get the filepath of the bytestrings for the current test
        parent = PACKAGE_ROOT / "tests" / "bytestring" / "txt"
        bytestrings_path = parent / f"{fn_hash}.py"

        if mode == "unit":
            mock_server_subproc = BytestringClientStubs.mock_server_subproc()
            mock_server_subproc.write(bytestrings_path)
            # comm_client = BytestringClientStubs.comm_client()
            # await comm_client.load_bytestrings(path=bytestrings_path)
        elif mode == "demo":
            client.export_bytestrings(path=bytestrings_path)

        return client
