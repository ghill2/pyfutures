import asyncio

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from unittest.mock import Mock

# from pyfutures.adapter..client.objects import IBTradeTick
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import Frequency
from pyfutures.client.enums import WhatToShow
from pyfutures.tests.demo.client.stubs import ClientStubs
from pyfutures.tests.bytestring.stubs import BytestringClientStubs
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
async def test_request_data(event_loop, mode):
    client = await BytestringClientStubs(mode=mode, loop=event_loop).client(
        loop=event_loop
    )

    await client.connect()
    await client.request_market_data_type(4)
    cont_contract = create_cont_contract()
    contract = await client.request_front_contract(cont_contract)

    end_time = pd.Timestamp("2024-03-22 16:30:00+00:00")

    quote_ticks = client.request_quote_ticks(
        contract=contract,
        end_time=end_time,
        count=50,
    )

    first_tick = await client.request_first_quote_tick(contract=contract)

    last_tick = await client.request_last_quote_tick(
        contract=contract,
    )

    trade_ticks = await client.request_trade_ticks(
        contract=contract,
        start_time=end_time - pd.Timedelta(hours=1),
        end_time=end_time,
        count=50,
    )

    bars = await client.request_bars(
        contract=contract,
        bar_size=BarSize._1_MINUTE,
        what_to_show=WhatToShow.TRADES,
        duration=Duration(step=1, freq=Frequency.DAY),
        end_time=end_time,
    )

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
