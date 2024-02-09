import pytest

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from ibapi.contract import Contract as IBContract
from pyfutures.adapters.interactive_brokers.enums import WhatToShow

@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    so sample one tick every hour in the liquid session
    """

    await client.connect()
    
    
    row = IBTestProviderStubs.universe_rows(filter=["ECO"])[0]
    
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
    
    
    