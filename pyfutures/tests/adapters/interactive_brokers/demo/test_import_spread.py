import pytest
import random
import time
import pandas as pd

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from ibapi.contract import Contract as IBContract
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from nautilus_trader.core.uuid import UUID4

@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    so sample one tick every hour in the liquid session
    """
    
    row = IBTestProviderStubs.universe_rows(filter=["ECO"])[0]
    
    await client.connect()
    
    contract = await client.request_front_contract(row.contract_cont)
    
    start_date = await client.request_head_timestamp(
        contract=contract,
        what_to_show=WhatToShow.BID_ASK,
    )
    
    times = row.liquid_schedule.to_date_range(
        start_date=start_date,
        interval=pd.Timedelta(hours=1),
    )
    times = times[::-1]
    
    seconds_in_hour = 3600
    milliseconds_in_hour = 3_600_000
    
    spreads = []
    for i, ts in enumerate(times):
        
        random_second = random.randint(0, seconds_in_hour - 2)
        start_time = ts + pd.Timedelta(seconds=random_second)
        end_time = ts + pd.Timedelta(hours=1)
        
        quotes = await client.request_quote_ticks(
            name=str(UUID4()),
            contract=contract,
            start_time=start_time,
            end_time=end_time,
            count=1,
        )
        if len(quotes) == 0:
            continue
        quote = quotes[0]
        spread = quote.ask_price - quote.bid_price
        spreads.append(spread)
        print(f"{i}/{len(times)} {row.instrument_id} {start_time} {spread}")
        
    
    