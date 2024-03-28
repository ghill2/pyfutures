import datetime
import time

import joblib
import pandas as pd
import pytest
from nautilus_trader.model.identifiers import InstrumentId
from pytest import ExitCode

from pyfutures.adapter.parsing import contract_to_instrument_id
from pyfutures.tests.test_kit import IBTestProviderStubs


class ExecutionFilledRunner:
    def __init__(self):
        sessions = IBTestProviderStubs.sessions()

        self._sessions = {}

        for session in sessions:
            start_time = datetime.time(
                session.start_time.hour + 1, session.start_time.minute
            )
            end_time = session.end_time

            assert start_time < end_time
            # assert (end_time - start_time) > pd.Timedelta(minutes=90)
            self._sessions[start_time] = session

    def run_all(self):
        while True:
            now_time = pd.Timestamp.utcnow().floor("T").time()
            self.run(now_time)
            time.sleep(30)

    def run(self, now_time: datetime.time):
        session = self._sessions.pop(now_time, None)
        print(now_time, session)

        if session is None:
            return

        contracts = session.contracts()

        excluded = "YIW"
        contracts = [x for x in contracts if x.tradingClass not in excluded]

        instrument_ids = [contract_to_instrument_id(contract) for contract in contracts]

        self._run(instrument_ids)

    def _run(self, instrument_ids: list[InstrumentId]) -> None:
        joblib.Parallel(n_jobs=-1, backend="loky", verbose=100)(
            joblib.delayed(_run_test)(instrument_id) for instrument_id in instrument_ids
        )


def _run_test(instrument_id: InstrumentId):
    import logging
    from pathlib import Path

    log_path = Path(__file__).parent / (r"logs/" + instrument_id.value + ".log")
    log_path_pytest = Path(__file__).parent / (
        r"logs/" + instrument_id.value + "_test.html"
    )

    retcode = pytest.main(
        [
            "--timeout=28800",  # 8 hours
            "-rP",
            "--tb=native",
            "--capture=tee-sys",
            "pytower/tests/adapters/interactive_brokers/demo/test_execution.py",
            "--instrument-id",
            instrument_id.value,
            "--file-logging",
            "True",
            "--file-log-path",
            str(log_path),
            "--html",
            str(log_path_pytest),
            "--self-contained-html",
            "--log-cli-level",
            str(logging.DEBUG),
            "-vv",
            "-k",
            "TestInteractiveBrokersExecutionFilled",
        ],
    )

    if retcode == ExitCode.TESTS_FAILED:
        import shutil

        shutil.move(
            str(log_path),
            str(log_path.with_name(log_path.stem + "_failed" + log_path.suffix)),
        )
        shutil.move(
            str(log_path_pytest),
            str(
                log_path_pytest.with_name(
                    log_path_pytest.stem + "_failed" + log_path.suffix
                )
            ),
        )


if __name__ == "__main__":
    runner = ExecutionFilledRunner()

    runner.run_all()
