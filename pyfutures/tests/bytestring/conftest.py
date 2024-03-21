import pytest


def pytest_addoption(parser):
    parser.addoption("--demo", action="store_true", help="")
    parser.addoption("--unit", action="store_true", help="")


@pytest.fixture
def mode(request):
    unit = request.config.getoption("--unit")
    demo = request.config.getoption("--demo")

    assert unit and demo, "--unit --demo are mutually exclusive"
    if unit:
        return "unit"
    if demo:
        return "demo"
    return False
