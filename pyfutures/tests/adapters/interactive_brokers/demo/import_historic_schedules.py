from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.adapters.interactive_brokers.parsing import instrument_id_to_contract
from nautilus_trader.model.identifiers import InstrumentId

import pytest
from pathlib import Path

@pytest.mark.asyncio()
async def test_import_historic_schedules(client):
    
    await client.connect()
    
    universe = IBTestProviderStubs.universe_dataframe()
    parent_out = Path("/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/schedules")
    for row in universe.itertuples():
        # instrument_id = InstrumentId.from_str(f"{row.trading_class}-{row.symbol}.{row.exchange}")
        
        instrument_id = InstrumentId.from_str("ZC-ZC.CBOT")
        contract = instrument_id_to_contract(instrument_id)
        
        contract = await client.request_front_contract(contract)
        sessions = await client.request_historical_schedule(contract=contract)
        
        print(len(sessions))
        
        sessions.to_csv(parent_out / "ZC.csv", index=False)
        exit()
    