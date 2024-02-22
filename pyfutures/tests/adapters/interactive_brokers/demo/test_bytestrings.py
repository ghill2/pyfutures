

def test_bytestring(bytestring_client):
    IBContract(secType="FUT", exchange="SNFE", symbol="XT", tradingClass="XT", currency="AUD")
    bytestring_client.get_contract_details()
