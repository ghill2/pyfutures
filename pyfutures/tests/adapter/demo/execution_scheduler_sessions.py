import pandas as pd
import pytest

from pyfutures.adapters.interactive_brokers.parsing import contract_to_instrument_id
from pyfutures.data.schedule.factory import MarketScheduleFactory
from pyfutures.tests.test_kit import IBTestProviderStubs


if __name__ == "__main__":
    """
    Schedule non-filled execution tests.
    """
    # [17, 18, 19, 21, 22, 23]
    sessions = IBTestProviderStubs.sessions(names=[21, 22, 23])

    start_time = min(session.start_time for session in sessions)
    end_time = min(session.end_time for session in sessions)
    """
    TODO:
    VIS.CFE: The contract's last trading time has passed.
    YIW[H29].CBOT: RuntimeError: Instrument not found, contract not available on IB that far away
    add special handling for price increment on IBEX, 5 instead of 1
    """

    contracts = []
    for session in sessions:
        contracts.extend(session.contracts())

    excluded = "YIW"
    contracts = [x for x in contracts if x.tradingClass not in excluded]

    # filtered = ("DJUSRE.CBOT")
    # instrument_ids = [x for x in instrument_ids if x.value in filtered]

    count = 0

    for contract in contracts:
        instrument_id = contract_to_instrument_id(contract)
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
                "TestInteractiveBrokersExecutionSessions",
            ],
        )
        count += 1
        # if count == 5:
        #     exit()

    # ["--instrument-id", "D.ICEEUSOFT"]

    exit()

    # RUST_BACKTRACE=1 python -m pytest -rP --tb=native --capture=tee-sys

    data = {}
    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        # instrument = await instrument_provider.load_async(instrument_id)

        # print(f"Processing {instrument_id}...")

        calendar = MarketScheduleFactory.from_symbol(instrument_id.symbol, open_offset=1)
        if calendar is None:  # no calendar for symbol
            continue

        now = pd.Timestamp.utcnow()

        if calendar.is_open(now) and calendar.time_until_close(now) < pd.Timedelta(hours=1):
            test_start_time = now
        else:
            test_start_time = calendar.next_open(now)

        data[instrument_id.value] = test_start_time
        # print(instrument_id, test_start_time)

    data = dict(sorted(data.items(), key=lambda item: item[1]))

    # data = {k: data[k] for k in sorted(data)}  # sort dict
    print(data)

    # get front contracts of universe

    # data[instrument_id] = test_start_time
    # else:
    #     test_start_time = calendar.next_open(now)
    # print(instrument_id, test_start_time)

    # data = dict(sorted(data.items(), key=lambda item: item[1]))
    # for instrument_id in IBTestProviderStubs.universe_instrument_ids():
    # calendar = MarketScheduleFactory.from_market_hours(instrument_id.symbol)

    # assert calendar is not None
    # if calendar.is_open(now) and calendar.time_until_close(now) >= pd.Timedelta(minutes=10):
    #     instrument_ids.append(chain.instrument_id)
