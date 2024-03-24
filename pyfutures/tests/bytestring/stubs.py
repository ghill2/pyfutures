import inspect
import sys
from pathlib import Path

from ibapi.contract import Contract as IBContract

from pyfutures import PACKAGE_ROOT
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.logger import init_logging


# TOMORROW: it seems to hang when i do --demo 1 try, then --unit 1 try, then --unit again..
# try comparing bytestrings after 1st unit and then 2nd unit ? maybe its rewriting


def create_cont_contract() -> IBContract:
    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "CONTFUT"
    return contract


# class SingletonMeta(type):
#     """
#     Metaclass that creates a Singleton class.
#     """
#
#     _instances = {}
#
#     def __call__(cls, *args, **kwargs):
#         """
#         Overrides the default __call__ method to ensure only one instance exists.
#         """
#         if cls not in cls._instances:
#             instance = super().__call__(*args, **kwargs)
#             cls._instances[cls] = instance
#         return cls._instances[cls]
#
#
# class BytestringClientStubs(metaclass=SingletonMeta):
#     _mock_server_subproc: MockServerSubproc | None = None
#     _client: InteractiveBrokersClient | None = None
#
#     def __init__(self, mode, loop):
#         self._mode = mode
#         self._loop = loop
#
#     async def client(self, *args, **kwargs):
#         if self._client is None:
#             port = 8890 if self._mode == "unit" else 4002
#             self._client = InteractiveBrokersClient(port=port, *args, **kwargs)
#
#         self._loop.set_debug(True)
#         init_logging()
#         caller_frame = sys._getframe(1)
#
#         # ensure caller function name begins with test_
#         caller_fn_name = caller_frame.f_code.co_name
#         assert caller_fn_name.startswith("test_")
#
#         source = inspect.getsource(caller_frame)
#
#         # Get the filepath of the bytestrings for the current test
#         caller_filename = Path(inspect.getfile(caller_frame)).stem
#         parent = PACKAGE_ROOT / "tests" / "bytestring" / "txt"
#         bytestring_path = parent / f"{caller_filename}={caller_fn_name}.json"
#
#         if self._mode == "unit":
#             if self._mock_server_subproc is None:
#                 # create mock_server subproc and wait until ready
#                 self._mock_server_subproc = MockServerSubproc(loop=self._loop)
#                 await self._mock_server_subproc.start()
#
#             # load bytestrings for the test and wait until they have loaded
#             _log_bytestrings(bytestring_path)
#             await self._mock_server_subproc.load_bytestrings(path=str(bytestring_path))
#
#         if self._mode == "export":
#             assert not bytestring_path.exists(), "Bytestring path "
#             self._client.conn.protocol.enable_bytestrings()
#             yield self._client
#             # tear down
#             self._client.conn.protocol.export_bytestrings(path=bytestring_path)
#
#         yield self._client
#
#         # remove empty lines and leading trailing whitespace
# lines = [line.strip() for line in source.splitlines() if line != ""]
# # remove async sleep and comments
# lines = [
#     line
#     for line in lines
#     if not line.startswith("#") and not line.startswith("await asyncio.sleep(")
# ]
# clean_fn_string = "\n".join(lines)
# Hash the cleaned string using a cryptographic hash function (e.g., SHA-256)
# fn_hash = hashlib.sha256(clean_fn_string.encode("utf-8")).hexdigest()
