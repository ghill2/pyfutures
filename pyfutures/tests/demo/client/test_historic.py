import pandas as pd
import logging
import pytest
from ibapi.contract import Contract as IBContract
import sys
# from nautilus_trader.core.datetime import secs_to_nanos
# from nautilus_trader.core.datetime import unix_nanos_to_dt

from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import WhatToShow
from pyfutures.client.historic import InteractiveBrokersBarClient
from pyfutures.client.parsing import parse_datetime


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_contract():
    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "CONTFUT"
    return contract


@pytest.mark.asyncio()
async def test_request_bars_downloads_expected(client):

    await client.connect()

    historic = InteractiveBrokersBarClient(client=client, delay=3)

    await client.request_market_data_type(4)

    contract = get_contract()

    bars = await historic.request_bars(
        contract=contract,
        bar_size=BarSize._1_DAY,
        what_to_show=WhatToShow.BID_ASK,
        start_time=pd.Timestamp("2024-01-10 00:00:00+00:00"),
        end_time=pd.Timestamp("2024-01-11 00:00:00+00:00"),
        cache=False
    )

@pytest.mark.asyncio()
async def test_request_ticks_downloads_expected(client):

    await client.connect()

    historic = InteractiveBrokersBarClient(client=client, delay=3)

    await client.request_market_data_type(4)

    contract = get_contract()

    ticks = await historic.request_quote_ticks(
        contract=contract,
        start_time=pd.Timestamp("2024-01-10 00:00:00+00:00"),
        end_time=pd.Timestamp("2024-01-11 00:00:00+00:00"),
    )
    print(ticks)




    # assert len(bars) == 34478
    # assert str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"
    #


# @pytest.mark.asyncio()
# async def test_all_bar_sizes(client):
#
#     await client.connect()
#
#     historic = InteractiveBrokersHistoric(client=client, delay=3)
#
#     await client.request_market_data_type(4)
#
#     contract = get_contract()
#     for bar_size in [BarSize._1_DAY, BarSize._1_HOUR, BarSize._1_MINUTE, BarSize._5_SECOND]:
#         print("===============================================================")
#         print(str(bar_size))
#         bars = await historic.request_bars(
#         contract=contract,
#         bar_size=bar_size,
#         what_to_show=WhatToShow.BID_ASK,
#         start_time=pd.Timestamp("2024-01-10 00:00:00+00:00"),
#         end_time=pd.Timestamp("2024-01-11 00:00:00+00:00"),
#         cache=False
#         )
    # assert len(bars) == 34478
    # assert str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"


######## TODO: Fix These Broken Tests #############

@pytest.mark.asyncio()
async def test_daily_downloads_expected(client):
    await self.client.connect()

    contract = IBContract()
    contract.conId = 452341897
    contract.symbol = "ALI"
    contract.exchange = "COMEX"             

    # for contract in IBTestProviderStubs.universe_contracts():
    df = await self.historic.download(
        contract=contract,
        bar_size=BarSize._1_MINUTE,
        # bar_size=BarSize._5_SECOND,
        what_to_show=WhatToShow.TRADES,
    )

    # assert len(bars) == 250
    # assert str(unix_nanos_to_dt(secs_to_nanos(int(bars[0].date)))) == "2023-07-17 20:59:00+00:00"

@pytest.mark.asyncio()
async def test_request_quote_ticks_dc(self, client):
    historic = InteractiveBrokersBarClient(client=client)


    start_time = pd.Timestamp("2023-02-13 14:30:00+00:00")
    end_time = pd.Timestamp("2023-02-13 22:00:00+00:00")

    await client.connect()
    quotes = await historic.request_quote_ticks(
        contract=contract,
        start_time=start_time,
        end_time=end_time,
    )
    assert all([parse_datetime(q.time) >= start_time and parse_datetime(q.time) < end_time for q in quotes])
    assert parse_datetime(quotes[0].time) == pd.Timestamp("2023-02-13 14:34:11+00:00")
    assert parse_datetime(quotes[-1].time) == pd.Timestamp("2023-02-13 21:44:26+00:00")

@pytest.mark.asyncio()
async def test_request_quote_ticks_zn(self, client):
    contract = IBContract()
    contract.tradingClass = "ZN"
    contract.symbol = "ZN"
    contract.exchange = "CBOT"
    contract.secType = "CONTFUT"
    start_time = pd.Timestamp("2024-01-18 17:50:00-00:00")
    end_time = pd.Timestamp("2024-01-18 18:00:00-00:00")
    historic = InteractiveBrokersBarClient(client=client, delay=2)

    await client.connect()
    quotes = await historic.request_quote_ticks(
        contract=contract,
        start_time=start_time,
        end_time=end_time,
    )

    assert len(quotes) == 6772
    assert quotes[0] == pd.Timestamp("2024-01-18 17:50:00+00:00")
    assert quotes[-1] == pd.Timestamp("2024-01-18 17:59:59+00:00")
