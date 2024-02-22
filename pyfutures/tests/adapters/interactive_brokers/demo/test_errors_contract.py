import pytest


@pytest.mark.asyncio
async def test_get_contract_details_with_warning_after(ib_client):
    """
    sometimes warning and received before or after the response
    the warning can be before or after the response
    IBContract(secType="FUT", exchange="SNFE", symbol="XT", tradingClass="XT", currency="AUD")
    """
    req_id = 10000
    ib_client._request_id_seq = 10000

    contract = IBTestContractStubs.aapl_equity_contract()
    details = IBTestContractStubs.aapl_equity_contract_details()
    ib_client._eclient.reqContractDetails = Mock()

    # Act
    with patch("asyncio.wait_for"):
        await ib_client.get_contract_details(contract)

    request = ib_client._requests.get(req_id=req_id)

    ib_client.contractDetails(req_id=req_id, contract_details=details)
    ib_client.contractDetailsEnd(req_id)
    ib_client.error(
        req_id,
        2121,
        "Warning: 2 products are trading on the basis other than currency price",
    )

    assert request.result[0] == details


@pytest.mark.asyncio
async def test_get_contract_details_with_warning_before(ib_client):
    """
    sometimes warning and received before or after the response
    the warning can be before or after the response
    IBContract(secType="FUT", exchange="SNFE", symbol="XT", tradingClass="XT", currency="AUD")
    """
    req_id = 10000
    ib_client._request_id_seq = 10000

    contract = IBTestContractStubs.aapl_equity_contract()
    details = IBTestContractStubs.aapl_equity_contract_details()
    ib_client._eclient.reqContractDetails = Mock()

    # Act
    with patch("asyncio.wait_for"):
        await ib_client.get_contract_details(contract)

    request = ib_client._requests.get(req_id=req_id)

    ib_client.error(
        req_id,
        2121,
        "Warning: 2 products are trading on the basis other than currency price",
    )

    ib_client.contractDetails(req_id=req_id, contract_details=details)
    ib_client.contractDetailsEnd(req_id)

    assert request.result[0] == details
