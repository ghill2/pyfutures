import asyncio
import pickle
import pytest
import time
from collections import deque
from pathlib import Path

import ib_insync as ibs
from ib_insync.util import sleep, logToConsole, logToFile
import pandas as pd
from ib_insync import Contract
from ibapi.common import BarData, HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from nautilus_trader.adapters.interactive_brokers.common import IBContractDetails
from nautilus_trader.common.component import Logger
import logging
from pyfutures.client.objects import ClientException
from pyfutures.adapter.enums import (
    BarSize,
    Duration,
    Frequency,
    WhatToShow,
)
from pyfutures.adapter.parsing import details_to_instrument_id
from pyfutures.tests.test_kit import IBTestProviderStubs

# logger = logging.getLogger("ib_insync_root")
# logger.setLevel(logging.DEBUG)
pytestmark = pytest.mark.ignore

cachedir = Path.home() / "Desktop" / "ibinsync_cache" / "request_bars"
cachedir.mkdir(parents=True, exist_ok=True)


ib = ibs.IB()
ibs.util.allowCtrlC()
ib.connect(host="127.0.0.1", port=4002, clientId=345, timeout=20)
ib.RequestTimeout = 60.0
logToConsole(level=logging.INFO)
logToFile(level=logging.INFO, path=Path.home() / "Desktop" / "ib_insync.log")

rows = IBTestProviderStubs.universe_rows()

ib.reqMarketDataType(4)



def key_builder(
    detail: IBContractDetails,
    bar_size: BarSize,
    what_to_show: WhatToShow,
    duration: Duration,
    end_time: pd.Timestamp,
):
    # https://blog.xam.de/2016/07/standard-format-for-time-stamps-in-file.html
    instrument_id = details_to_instrument_id(detail)
    end_time_str = end_time.isoformat().replace(":", "_")
    start_time = end_time - duration.to_timedelta()
    start_time_str = start_time.isoformat().replace(":", "_")
    return f"{instrument_id}-{what_to_show.value}-{str(bar_size)}-{duration.value}-{start_time_str}-{end_time_str}"


for row in rows:
    contract = ibs.Contract(
        tradingClass=row.contract_cont.tradingClass,
        exchange=row.contract_cont.exchange,
        secType="CONTFUT",
        symbol=row.contract_cont.symbol,
    )
    print(contract)
    duration = Duration(step=1, freq=Frequency.DAY)
    what_to_show = WhatToShow.BID_ASK
    bar_size = BarSize._1_MINUTE

    head_timestamp = ib.reqHeadTimeStamp(
        contract=contract, whatToShow=what_to_show.value, useRTH=True, formatDate=2
    )

    total_bars = []
    interval = duration.to_timedelta()

    print(f"-> req_head_timestamp: {head_timestamp}")
    # if start_time is None or start_time < head_timestamp:
    start_time = head_timestamp

    end_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D")

    while end_time >= start_time:
        print(f"--> {end_time} -> {end_time - interval}")
        key = key_builder(
            detail=IBContractDetails(contract=contract),
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=duration,
            end_time=end_time,
        )

        pkl_path = cachedir / f"{key}.pkl"
        if pkl_path.exists():
            end_time = end_time - interval
            continue

        start = time.perf_counter()

        request_bars_params = dict(
            contract=contract,
            endDateTime=end_time,
            durationStr=duration.value,
            barSizeSetting=str(bar_size),
            whatToShow=what_to_show.value,
            useRTH=True,
            formatDate=2,
        )
        print(f"---> {request_bars_params=}")
        bars = ib.reqHistoricalData(**request_bars_params)
        stop = time.perf_counter()
        elapsed = stop - start
        print(f"---> Elapsed time: {elapsed:.2f}")

        if len(bars) > 0:
            print("---> Writing bars: ", len(bars))
            with open(pkl_path, "wb") as f:
                pickle.dump(bars, f)

        end_time = end_time - interval
        sleep(2)


ib.disconnect()
