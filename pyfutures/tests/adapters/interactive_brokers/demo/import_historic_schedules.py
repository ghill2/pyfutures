from pathlib import Path

import pytest
from ibapi.contract import Contract

from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs


@pytest.mark.asyncio()
async def test_import_historic_schedules(client):
    await client.connect()

    universe = IBTestProviderStubs.universe_dataframe()
    parent_out = Path("/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/schedules")
    for row in universe.itertuples():
        path = parent_out / f"{row.trading_class}.csv"
        if path.exists():
            continue

        try:
            contract = await client.request_front_contract(row.contract)
            assert type(contract) is Contract
            sessions = await client.request_historical_schedule(contract=contract)
        except ClientException as exc:
            if exc.code == 200:
                print(f"{row.trading_class}")
            else:
                raise exc

        print(f"{row.trading_class}: {len(sessions)} sessions")

        sessions.to_csv(path, index=False)

        path = parent_out / f"{row.trading_class}.parquet"
        sessions.to_parquet(path, index=False)
