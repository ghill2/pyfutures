import pytest
from ibapi.contract import ContractDetails as IBContractDetails

from pyfutures.client.objects import ClientException


@pytest.mark.asyncio()
async def test_request_contract_details_raises_exception(client, contract):
    await client.connect()
    contract.secType = "invalid_secType"
    with pytest.raises(ClientException) as e:
        await client.request_contract_details(contract)
        assert e.code == 321


@pytest.mark.asyncio()
async def test_request_contract_details_returns_expected(client, contract):
    await client.connect()
    results = await client.request_contract_details(contract)
    assert isinstance(results, list)
    assert all(isinstance(result, IBContractDetails) for result in results)


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_request_last_contract_month():
    pass


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_request_front_contract_details():
    pass


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_request_front_contract():
    pass
