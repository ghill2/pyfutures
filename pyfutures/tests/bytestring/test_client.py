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
async def test_request_accounts(client):
    await client.connect()
    await client.request_accounts()


@pytest.mark.asyncio()
async def test_request_next_order_id(client):
    await client.connect()
    await client.request_next_order_id()
    await client.request_next_order_id()
