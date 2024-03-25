import asyncio

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from unittest.mock import Mock
import pytest_asyncio

# from pyfutures.adapter..client.objects import IBTradeTick
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import Frequency
from pyfutures.client.enums import WhatToShow
from pyfutures.tests.bytestring.stubs import create_cont_contract


# @pytest.mark.asyncio()
# async def test_request_head_timestamp_single(event_loop):
#     client = ClientStubs.client(loop=event_loop)
#     await client.connect()
#     contract = create_cont_contract()
#
#     timestamp = await client.request_head_timestamp(
#         contract=contract,
#         what_to_show=WhatToShow.BID,
#     )
#     assert str(timestamp) == "2020-09-16 13:30:00+00:00"


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
    assert bar.open == 16.05
    assert bar.close == 16.16
    bar = bars[1]
    assert bar.date == "20240319"
    assert bar.timestamp == pd.Timestamp("2024-03-19 00:00:00+00:00")
    assert bar.high == 16.41
    assert bar.low == 16.2
    assert bar.open == 16.2
    assert bar.close == 16.29
    bar = bars[2]
    assert bar.date == "20240320"
    assert bar.timestamp == pd.Timestamp("2024-03-20 00:00:00+00:00")
    assert bar.high == 16.44
    assert bar.low == 16.08
    assert bar.open == 16.39
    assert bar.close == 16.17
    bar = bars[3]
    assert bar.date == "20240321"
    assert bar.timestamp == pd.Timestamp("2024-03-21 00:00:00+00:00")
    assert bar.high == 16.22
    assert bar.low == 15.71
    assert bar.open == 16.2
    assert bar.close == 15.74
    bar = bars[4]
    assert bar.date == "20240322"
    assert bar.timestamp == pd.Timestamp("2024-03-22 00:00:00+00:00")
    assert bar.high == 15.82
    assert bar.low == 15.56
    assert bar.open == 15.81
    assert bar.close == 15.57


@pytest.mark.asyncio()
async def test_request_quote_ticks(client, dc_cont_contract):
    await client.connect()
    await client.request_market_data_type(4)
    contract = await client.request_front_contract(dc_cont_contract)
    ticks = await client.request_quote_ticks(
        contract=contract,
        end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
        count=5,
    )
    date 20240318
    timestamp 2024-03-18 00:00:00+00:00
    high 16.3
    low 15.95
    open 16.05
    close 16.16
    date 20240319
    timestamp 2024-03-19 00:00:00+00:00
    high 16.41
    low 16.2
    open 16.2
    close 16.29
    date 20240320
    timestamp 2024-03-20 00:00:00+00:00
    high 16.44
    low 16.08
    open 16.39
    close 16.17
    date 20240321
    timestamp 2024-03-21 00:00:00+00:00
    high 16.22
    low 15.71
    open 16.2
    close 15.74
    date 20240322
    timestamp 2024-03-22 00:00:00+00:00
    high 15.82
    low 15.56
    open 15.81
    close 15.57

    for bar in ticks:
        print(bar.time)
        print(bar.timestamp)
        print(bar.priceBid)
        print(bar.priceAsk)
        print(bar.sizeBid)
        print(bar.sizeAsk)


# first_tick = await client.request_first_quote_tick(contract=contract)

# last_tick = await client.request_last_quote_tick(
#     contract=contract,
# )

# trade_ticks = await client.request_trade_ticks(
#     contract=contract,
#     start_time=end_time - pd.Timedelta(hours=1),
#     end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
#     count=50,
# )


# assert quote ticks
# assert len(quotes) == 54
# assert all(isinstance(quote, HistoricalTickBidAsk) for quote in quotes)
#
# last quote_tick
# assert isinstance(last, HistoricalTickBidAsk)
#
# trade ticks
# assert len(trades) == 51
# assert all(isinstance(trade, IBTradeTick) for trade in trades)
#
# bars
# assert all(isinstance(bar, BarData) for bar in bars)
# assert len(bars) > 0
