# from pyfutures.adapter..client.objects import IBFuturesInstrument

import pytest
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price

from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth


init_logging(level_stdout=LogLevel.DEBUG)


@pytest.mark.asyncio()
async def test_load_with_instrument_id(instrument_provider):
    # Arrange
    instrument_id = InstrumentId.from_str("ZC-ZC=H24.CBOT")

    # Act
    instrument = await instrument_provider.load_contract(instrument_id)
    assert instrument is not None


@pytest.mark.asyncio()
async def test_load_with_safe_instrument_id(instrument_provider):
    # Arrange
    instrument_id = InstrumentId.from_str("MNTPX-TPXM=H24.OSE|JPN")

    # Act
    instrument = await instrument_provider.load_contract(instrument_id)
    assert instrument is not None


@pytest.mark.asyncio()
async def test_load_uses_chain_filter(instrument_provider):
    # Arrange
    instrument_provider._chain_filters = {
        "FMEU": lambda x: not x.contract.localSymbol.endswith("D"),
    }
    instrument_id = InstrumentId.from_str("M7EU-FMEU=H24.EUREX")

    # Act
    details = await instrument_provider.load_contract(instrument_id)

    # Assert
    assert details is not None


@pytest.mark.asyncio()
async def test_load_parsing_overrides_sets_expected(instrument_provider):
    # Arrange
    instrument_provider._parsing_overrides = {
        "MIX": {
            "price_precision": 0,
            "price_increment": Price(5, 0),
        },
    }
    instrument_id = InstrumentId.from_str("IBEX-MIX=F24.MEFFRV")

    # Act
    instrument = await instrument_provider.load_contract(instrument_id)

    # Assert
    assert instrument.price_precision == 0
    assert instrument.price_increment == 5


@pytest.mark.asyncio()
async def test_request_future_chain_details_returns_expected(instrument_provider):
    config = FuturesChainConfig(
        instrument_id="ZN-ZN.CBOT",
        hold_cycle="HMUZ",
        priced_cycle="HMUZ",
        roll_offset=-25,
        approximate_expiry_offset=19,
        carry_offset=1,
    )
    chain = FuturesChain(config=config)

    details_list = await instrument_provider.request_future_chain_details(chain)
    for details in details_list:
        assert ContractMonth.from_int(details.contractMonth) in chain.hold_cycle


@pytest.mark.asyncio()
async def test_find_with_contract_id_requests_instrument(instrument_provider):
    await instrument_provider.client.connect()

    contract = await instrument_provider.find_with_contract_id(564400671)

    assert contract.id == InstrumentId.from_str("D-RC=F24.ICEEUSOFT")


# @pytest.mark.asyncio()
# async def test_load_with_contract(instrument_provider):

#     await instrument_provider.client.connect()

#     contract = IBContract()
#     contract.conId = 564400671

#     await instrument_provider.load_async(contract)
#     contract = await instrument_provider.find_with_contract_id(564400671)

#     assert contract.info['contract'].conId == 564400671


# @pytest.mark.asyncio()
# async def test_load_with_futures_instrument(client):
#     """
#     MES-MES-Z23.CME = 20231215
#     R-R-Z23.ICEEU = 20231227
#     ZC
#     """
#     """
#     CGB-CGB-Z23.CDE
#     """
#     # contract = IBContract()
#     # contract.symbol = "CAC40"
#     # contract.exchange = "MONEP"
#     # contract.secType = "FUT"
#     # contract.includeExpired = False

#     contract = IBContract()
#     contract.symbol = "M7EU"
#     contract.exchange = "EUREX"
#     contract.tradingClass = "FMEU"
#     contract.secType = "FUT"
#     contract.includeExpired = False


#     details_list = await client.request_contract_details(contract)

#     for details in details_list:
#         # if details.contractMonth == "202312":
#         print(
#             # details.contract.symbol,
#             " | ".join(map(str, [
#                 details.contract.localSymbol,
#                 details.contract.tradingClass,
#                 details.contract.primaryExchange,
#                 details.contractMonth,
#                 details.contract.lastTradeDateOrContractMonth,
#                 details.realExpirationDate,
#                 # details.longName,
#                 details.contract.issuerId,
#                 details.marketName,

#                 # details.contractMonth,
#                 # details.contract.secType,
#                 details.contract.secIdType,
#                 details.contract.secId,
#                 details.contract.description,
#                 details.contract.multiplier,
#                 details.contract.currency,
#                 # details.underConId,
#                 # details.stockType,
#             ]))
#     )
