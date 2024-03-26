import pytest
# @pytest.mark.asyncio()
# async def test_request_account_summary(client):
#     # await client.connect()
#     summary = await client.request_account_summary()
#     print(summary)
#     # assert isinstance(summary, dict)


@pytest.mark.asyncio()
async def test_request_positions(client):
    await client.connect()
    positions = await client.request_positions()
    print(positions)


@pytest.mark.asyncio()
async def test_request_executions(client):
    await client.connect()
    executions = await client.request_executions()
    print(executions)


@pytest.mark.asyncio()
async def test_request_accounts(client):
    await client.connect()
    await client.request_accounts()


@pytest.mark.asyncio()
async def test_request_historical_schedule(client, contract):
    await client.connect()
    # contract = Contract()
    # contract.symbol = "SCI"
    # contract.localSymbol = "FEFF27"
    # contract.exchange = "SGX"
    # contract.secType = "FUT"
    # contract.includeExpired = False
    df = await client.request_historical_schedule(contract=contract)
    print(df.iloc[:49])


@pytest.mark.asyncio()
async def test_request_portfolio(client):
    await client.connect()
    await client.request_portfolio()
