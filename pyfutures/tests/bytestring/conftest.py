import asyncio
from pathlib import Path

import pytest
import pytest_asyncio

from pyfutures import PACKAGE_ROOT
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.logger import init_logging
from pyfutures.tests.bytestring.mock_server_subproc import MockServerSubproc

from typing import Dict
from pytest import StashKey, CollectReport
import pickle
from _pytest.mark import Mark
from ibapi.contract import Contract as IBContract


####


@pytest.fixture
def dc_contract():
    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "FUT"
    return contract


@pytest.fixture
def dc_cont_contract(dc_contract):
    dc_contract.secType = "CONTFUT"
    return dc_contract


# https://docs.pytest.org/en/latest/example/simple.html#making-test-result-information-available-in-fixtures
phase_report_key = StashKey[Dict[str, CollectReport]]()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    rep = yield

    # store test results for each phase of a call, which can
    # be "setup", "call", "teardown"
    item.stash.setdefault(phase_report_key, {})[rep.when] = rep

    return rep


# to handle non default client params:
# https://stackoverflow.com/a/77674640
def get_marker(request, name) -> Mark:
    markers = list(request.node.iter_markers(name))
    if len(markers) > 1:
        pytest.fail(f"Found multiple markers for {name}")
    return markers[0]


##########
def _log_bytestrings(path):
    print("==== BYTESTRINGS ====")
    _bstream = pickle.load(open(path, "rb"))
    for line in _bstream:
        print(line[0], line[1])
    print("==== =========== ====")


def pytest_addoption(parser):
    parser.addoption("--export", action="store_true", help="")
    parser.addoption("--unit", action="store_true", help="")


@pytest.fixture(scope="session")
def mode(request):
    unit = request.config.getoption("--unit")
    demo = request.config.getoption("--export")
    if unit and demo:
        raise ValueError("--unit --export are mutually exclusive")
    if unit:
        return "unit"
    if demo:
        return "export"
    return False


# https://stackoverflow.com/questions/75161749/scopemismatch-when-i-try-to-make-setup-teardown-pytest-function
@pytest.fixture(scope="session")
def event_loop():
    init_logging()
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def mock_server_subproc(event_loop, mode):
    if mode == "unit":
        mock_server_subproc = MockServerSubproc(loop=event_loop)
        await mock_server_subproc.start()
        return mock_server_subproc
    return None


@pytest_asyncio.fixture
async def client(request, event_loop, mock_server_subproc, mode):
    """
    Always instantiate a new InteractiveBrokersClient for the test
    for unit tests, the request ids of the bytestrings need to match the request ids sent from the client during the test
    If a client is not instantiated new every time, the request ids will increment across tests

    every test function needs to start with connect()
    as the mock_server is reset to initial state before each test starts
    """
    print("================= GETTING CLIENT ==================")

    test_filename = Path(request.fspath).stem
    test_fn_name = request.node.originalname
    parent = PACKAGE_ROOT / "tests" / "bytestring" / "txt"
    bytestring_path = parent / f"{test_filename}={test_fn_name}.json"

    if mode == "unit":
        assert (
            bytestring_path.exists()
        ), f"Bytestrings do not exist at path: {bytestring_path}"
        await mock_server_subproc.perform_command(
            cmd="load_bytestrings", value=str(bytestring_path)
        )

    port = 8890 if mode == "unit" else 4002
    bytestring_client = InteractiveBrokersClient(loop=event_loop, port=port)

    if mode == "export":
        assert (
            not bytestring_path.exists()
        ), f"Bytestrings already exist for path: {bytestring_path}"

        bytestring_client.conn.protocol.enable_bytestrings()

    yield bytestring_client
    # if mode == "unit":
    # reset / cleanup mock server for the next test
    # await mock_server_subproc.perform_command(cmd="reset", value="")

    if mode == "export":
        report = request.node.stash[phase_report_key]

        # if the test failed
        if "call" not in report:
            return

        if report["call"].outcome == "passed":
            bytestring_client.conn.protocol.export_bytestrings(path=bytestring_path)
