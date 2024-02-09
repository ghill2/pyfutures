import pytest
import pandas as pd

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from ibapi.contract import Contract as IBContract
from pyfutures.adapters.interactive_brokers.enums import WhatToShow

@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    so sample one tick every hour in the liquid session
    """
    
    row = IBTestProviderStubs.universe_rows(filter=["ECO"])[0]
    
    # get historical schedule
    
    times = row.liquid_schedule.to_date_range(
        start_date=pd.Timestamp("01-01-1993"),
        end_date=pd.Timestamp("01-01-2023"),
        frequency="1h"
    )
    for time in times:
        print(time.dayofweek, time)
        
    exit()
    
    # find liquid hours within historical schedule session
    
    # for each hour in the liquid hours, if the hour is in the session open, use it
    
    await client.connect()
    
    contract = await client.request_front_contract(row.contract)
    
    start = await client.request_head_timestamp(
        contract=contract,
        what_to_show=WhatToShow.BID_ASK,
    )
    print(start, contract)
       
    # quotes = await client.request_quote_ticks(
    #     contract=contract,
    #     end_time: pd.Timestamp = None
    #     end_time: pd.Timestamp = None
    #     # use_rth: bool = True,
    # )
    
    
    