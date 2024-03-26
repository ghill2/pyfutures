# @pytest.mark.asyncio()
# async def test_request_contract_details_raises_exception(event_loop):
#     client = ClientStubs.client(loop=event_loop)
#     await client.connect()
#     contract = Contract()
#     contract.secType = "invalid_secType"
#     contract.symbol = "D"
#     contract.exchange = "ICEEUSOFT"
#
#     with pytest.raises(ClientException) as e:
#         await client.request_contract_details(contract)
#         assert e.code == 321
