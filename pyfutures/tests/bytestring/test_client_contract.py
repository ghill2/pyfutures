import pytest
from ibapi.contract import ContractDetails as IBContractDetails


from pyfutures.client.objects import ClientException
from pyfutures.logger import init_ib_api_logging
import logging


@pytest.mark.asyncio()
async def test_request_contract_details_raises_exception(client, contract):
    init_ib_api_logging(level=logging.DEBUG)
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
