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
#
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
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def mock_server_subproc(event_loop, mode):
    if mode == "unit":
        mock_server_subproc = MockServerSubproc(loop=event_loop)
        await mock_server_subproc.start()
        return mock_server_subproc
        # tear down
    return None


@pytest_asyncio.fixture(scope="session")
async def bytestring_client(event_loop, mode):
    event_loop.set_debug(True)
    init_logging()
    port = 8890 if mode == "unit" else 4002
    return InteractiveBrokersClient(loop=event_loop, port=port)


@pytest_asyncio.fixture
async def client(request, bytestring_client, mock_server_subproc, mode):
    test_filename = Path(request.fspath).stem
    test_fn_name = request.node.originalname
    parent = PACKAGE_ROOT / "tests" / "bytestring" / "txt"
    bytestring_path = parent / f"{test_filename}={test_fn_name}.json"

    if mock_server_subproc is not None:  # --unit
        # load bytestrings for the test and wait until they have loaded
        # _log_bytestrings(bytestring_path)
        await mock_server_subproc.load_bytestrings(path=str(bytestring_path))

    if mode == "export":
        assert (
            not bytestring_path.exists()
        ), f"Bytestrings already exist for path: {bytestring_path}"

        bytestring_client.conn.protocol.enable_bytestrings()

        # def teardown_on_success():
        #     if request.node.result.passed:
        #         bytestring_client.conn.protocol.export_bytestrings(path=bytestring_path)
        #
        # request.addfinalizer(teardown_on_success)
        #
    yield bytestring_client

    if mode == "export":
        report = request.node.stash[phase_report_key]

        # if the test failed
        if "call" not in report:
            return

        if report["call"].outcome == "passed":
            bytestring_client.conn.protocol.export_bytestrings(path=bytestring_path)

        # if report["setup"].failed:
        #     print("setting up a test failed or skipped", request.node.nodeid)
        # elif ("call" not in report) or report["call"].failed:
        #     print("executing test failed or skipped", request.node.nodeid)
        #
        # report = request.config.pluginmanager.getplugin("pytest").lastitem.report
        # if report.passed:
