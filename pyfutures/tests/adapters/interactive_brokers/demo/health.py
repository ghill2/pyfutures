from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.adapters.interactive_brokers.parsing import create_contract
from nautilus_trader.model.identifiers import InstrumentId
from pyfutures.adapters.interactive_brokers.client.objects import ClientException

import pytest
from pathlib import Path
from ibapi.contract import Contract

@pytest.mark.asyncio()
async def test_request_front_contract_universe(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()
    
    universe = IBTestProviderStubs.universe_dataframe()
    for row in universe.itertuples():
        
        contract = create_contract(
            trading_class=row.trading_class,
            symbol=row.symbol,
            venue=row.exchange,
        )
        
        try:
            contract = await client.request_front_contract(contract)
        except ClientException as e:
            if e.code == 200:
                print(f"{row.trading_class}")
            else:
                raise e
        
@pytest.mark.asyncio()
async def test_request_front_contract_universe_fix(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()
        
    contract = Contract()
    contract.symbol = "ZC"
    contract.tradingClass = "ZC"
    contract.exchange = "CBOT"
    contract.secType = "FUT"
    contract.includeExpired = False
    
    try:
        contract = await client.request_front_contract(contract)
    except ClientException as e:
        if e.code == 200:
            print(f"{row.trading_class}")
        else:
            raise e
    