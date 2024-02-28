from collections import deque
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
import ib_insync as ibs
from ib_insync import Contract
import pandas as pd
import asyncio
from collections import deque
from nautilus_trader.adapters.interactive_brokers.common import IBContractDetails

import pandas as pd
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from nautilus_trader.common.component import Logger

from pyfutures.adapters.interactive_brokers.client.objects import ClientException

# from pyfutures.adapters.interactive_brokers.client.objects import TimeoutError
from pyfutures.adapters.interactive_brokers.client.client import (
    InteractiveBrokersClient,
)
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import bar_data_to_dict
from pyfutures.adapters.interactive_brokers.parsing import (
    historical_tick_bid_ask_to_dict,
)
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime

from pathlib import Path
from pyfutures.adapters.interactive_brokers.parsing import (
    contract_details_to_instrument_id,
)

from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
import pickle
import json
import logging
from typing import Iterable
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow


# logger = logging.getLogger("ib_insync_root")
# logger.setLevel(logging.DEBUG)


cachedir = Path.home() / "Desktop" / "ibinsync_cache" / "request_bars"
cachedir.mkdir(parents=True, exist_ok=True)


ib = ibs.IB()
ibs.util.allowCtrlC()
ib.connect(host="127.0.0.1", port=4002, clientId=345, timeout=20)

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
    instrument_id = contract_details_to_instrument_id(detail)
    end_time_str = end_time.isoformat().replace(":", "_")
    start_time = end_time - duration.to_timedelta()
    start_time_str = start_time.isoformat().replace(":", "_")
    return f"{instrument_id}-{what_to_show.value}-{str(bar_size)}-{duration.value}-{start_time_str}-{end_time_str}"


for row in rows:
    contract = ibs.Contract(
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

    print(f"--> req_head_timestamp: {head_timestamp}")
    # if start_time is None or start_time < head_timestamp:
    start_time = head_timestamp

    end_time = pd.Timestamp.utcnow()
    while end_time >= start_time:
        bars = ib.reqHistoricalData(
            contract,
            # endDateTime=datetime.datetime.now(),
            endDateTime="",
            durationStr=duration.value,
            barSizeSetting=str(bar_size),
            whatToShow=what_to_show.value,
            useRTH=True,
            formatDate=2,
        )
        key = key_builder(
            detail=IBContractDetails(contract=contract),
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=duration,
            end_time=end_time,
        )
        end_time = end_time - interval

        pkl_path = cachedir / f"{key}.pkl"
        with open(pkl_path, "wb") as f:
            pickle.dump(bars, f)
        print(len(bars))


ib.disconnect()
