from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from ibapi.contract import Contract as IBContract


@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    so sample one tick every hour in the liquid session
    """

    rows = IBTestProviderStubs.universe_rows()
    
    await client.connect()
    
    client.
    
    contract: IBContract,
    end_time: pd.Timestamp = None
    use_rth: bool = True,
    