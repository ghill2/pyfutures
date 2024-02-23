from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
init_logging(level_stdout=LogLevel.DEBUG)


def test_bytestring(bytestring_client):
    IBContract(secType="FUT", exchange="SNFE", symbol="XT", tradingClass="XT", currency="AUD")
    bytestring_client.get_contract_details()
