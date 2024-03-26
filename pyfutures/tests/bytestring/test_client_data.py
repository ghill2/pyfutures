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


@pytest.mark.skip(reason="TODO")
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
    tick = ticks[0]
    assert tick.time == 1711124972
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:32+00:00")
    assert tick.priceBid == 15.6
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 2
    assert tick.sizeAsk == 1

    tick = ticks[1]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 8
    assert tick.sizeAsk == 3

    tick = ticks[2]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 7
    assert tick.sizeAsk == 3

    tick = ticks[3]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 3
    assert tick.sizeAsk == 3

    tick = ticks[4]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 2
    assert tick.sizeAsk == 3

    tick = ticks[5]
    assert tick.time == 1711124999
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:59+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 4
    assert tick.sizeAsk == 3

    print(len(ticks))
    # assert len(ticks) == 51
    # assert all(isinstance(trade, IBTradeTick) for trade in trades)


@pytest.mark.skip(reason="universe WIP")
@pytest.mark.asyncio()
async def test_request_first_quote_tick_universe(client):
    await client.connect()

    rows = IBTestProviderStubs.universe_rows()
    for row in rows:
        if row.contract_cont.exchange not in ["CME", "CBOT", "NYMEX"]:
            continue

        await client.request_market_data_type(4)
        contract = await client.request_front_contract(row.contract_cont)

        # first_tick = await client.request_first_quote_tick(contract=contract)
        ticks = await client.request_first_quote_tick(
            contract=contract,
            # end_time=pd.Timestamp("2024-03-22 16:30:30+00:00"),
            # start_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
            # count=1,
        )

        print(len(ticks))
        if len(ticks) > 1:
            assert False

        for tick in ticks:
            print(tick.time)
            print(tick.timestamp)
            print(tick.priceBid)
            print(tick.priceAsk)
            print(tick.sizeBid)
            print(tick.sizeAsk)


@pytest.mark.skip(reason="Cant find start time value?")
@pytest.mark.asyncio()
async def test_request_first_quote_tick(client, dc_contract):
    await client.connect()

    await client.request_market_data_type(4)
    contract = await client.request_front_contract(dc_contract)

    # first_tick = await client.request_first_quote_tick(contract=contract)
    # ticks = await client.request_first_quote_tick(
    #     contract=contract,
    #     # end_time=pd.Timestamp("2024-03-22 16:30:30+00:00"),
    #     # start_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
    #     # count=1,
    # )
    print("WHAT_TO_SHOW")
    # 2022-03-31 22:00:00+00:00 <-- req_head
    # ticks = await client.request_quote_ticks(
    #     contract=contract,
    #     count=1000,
    #     end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
    #     whatToShow="BID_ASK",
    # )
    # ts = []
    # for detail in details:
    #     head_timestamp = await client.request_head_timestamp(
    #         contract=detail.contract, what_to_show=WhatToShow.BID_ASK
    #     )
    #     # print("HEAD TIMESTAMP", head_timestamp)
    #     ts.append((detail.contract.lastTradeDateOrContractMonth, head_timestamp))
    #     await asyncio.sleep(0.5)
    # print(ts)
    ticks = await client.request_quote_ticks(
        contract=contract,
        count=1000,
        start_time=pd.Timestamp("2022-03-31 22:00:00+00:00"),
        end_time=pd.Timestamp.utcnow(),
    )

    #
    # print(len(ticks))
    # if len(ticks) > 1:
    # assert False
    #
    for tick in ticks:
        print(tick.time)
        print(tick.timestamp)
        print(tick.priceBid)
        print(tick.priceAsk)
        print(tick.sizeBid)
        print(tick.sizeAsk)


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
