import pytest

from pyfutures.tests.test_kit import IBTestProviderStubs


if __name__ == "__main__":
    """
    Schedule cancel and accept execution tests that can run anytime regardless of a
    closed market.
    """

    sessions = IBTestProviderStubs.sessions()

    contracts = []
    for session in sessions:
        contracts.extend(session.contracts())
    contracts = list(reversed(contracts))

    excluded = "YIW"  # contract doesn't exist yet
    contracts = [x for x in contracts if x.instrument.tradingClass not in excluded]

    for contract in contracts:
        instrument_id = contract.instrument_id
        print(f"Running execution suite of {instrument_id}")

        retcode = pytest.main(
            [
                "--timeout=20",
                "-rP",
                "--tb=native",
                "--capture=tee-sys",
                "pytower/tests/adapters/interactive_brokers/demo/test_execution.py",
                "--instrument-id",
                instrument_id.value,
                "-vv",
                "-k",
                "TestInteractiveBrokersExecutionCancelAccept",
            ],
        )
