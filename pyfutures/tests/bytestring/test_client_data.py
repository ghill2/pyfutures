import asyncio

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from unittest.mock import Mock
import pytest_asyncio
from pyfutures.tests.test_kit import IBTestProviderStubs

# from pyfutures.adapter..client.objects import IBTradeTick
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import Frequency
from pyfutures.client.enums import WhatToShow

# QUOTETICKS:
# subscribe_quote_ticks (reqTickByTickData)
# - using to execute when running strategy
#
# BARS:
# subscribe_historical_bars (reqHistoricalData() keepUpToDate=True)
# - for daily, using while strats running
# request_bars (reqHistoricalData)
# - need _5_SECOND, DAILY, 1 HOUR
#
#
# We are NOT USING:
# subscribe realtime bars
# request_quote_ticks


# @pytest.mark.asyncio()
async def test_request_head_timestamp_single(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()
    contract = create_cont_contract()

    timestamp = await client.request_head_timestamp(
        contract=contract,
        what_to_show=WhatToShow.BID,
    )
    assert str(timestamp) == "2020-09-16 13:30:00+00:00"


@pytest.mark.asyncio()
async def test_request_bars(client, dc_cont_contract):
    await client.connect()
    await client.request_market_data_type(4)
    contract = await client.request_front_contract(dc_cont_contract)
    bars = await client.request_bars(
        contract=contract,
        bar_size=BarSize._1_DAY,
        what_to_show=WhatToShow.TRADES,
        duration=Duration(step=5, freq=Frequency.DAY),
        end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
    )
    bar = bars[0]
    assert bar.date == "20240318"
    assert bar.timestamp == pd.Timestamp("2024-03-18 00:00:00+00:00")
    assert bar.high == 16.3
    assert bar.low == 15.95
    assert bar.open == 16.01
    assert bar.close == 16.16
    bar = bars[1]
    assert bar.date == "20240319"
    assert bar.timestamp == pd.Timestamp("2024-03-19 00:00:00+00:00")
    assert bar.high == 16.41
    assert bar.low == 16.15
    assert bar.open == 16.15
    assert bar.close == 16.29
    bar = bars[2]
    assert bar.date == "20240320"
    assert bar.timestamp == pd.Timestamp("2024-03-20 00:00:00+00:00")
    assert bar.high == 16.45
    assert bar.low == 16.08
    assert bar.open == 16.35
    assert bar.close == 16.17
    bar = bars[3]
    assert bar.date == "20240321"
    assert bar.timestamp == pd.Timestamp("2024-03-21 00:00:00+00:00")
    assert bar.high == 16.3
    assert bar.low == 15.71
    assert bar.open == 16.18
    assert bar.close == 15.74
    bar = bars[4]
    assert bar.date == "20240322"
    assert bar.timestamp == pd.Timestamp("2024-03-22 00:00:00+00:00")
    assert bar.high == 15.85
    assert bar.low == 15.56
    assert bar.open == 15.76
    assert bar.close == 15.57

    assert all(isinstance(bar, BarData) for bar in bars)
    assert len(bars) > 0
