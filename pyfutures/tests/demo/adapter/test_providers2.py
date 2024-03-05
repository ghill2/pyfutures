# from pyfutures.adapter..client.objects import IBFuturesInstrument
import asyncio
from unittest.mock import Mock

import pytest
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.tests.test_kit import IBTestProviderStubs


@pytest.mark.asyncio()
async def test_load_with_instrument_id(instrument_provider):
    message = (
        b"10\x00-10\x00ZC\x00FUT\x0020240314-17:01:00\x000\x00\x00CBOT\x00USD\x00ZC   MAR 24\x00ZC\x00ZC\x00532513368\x000.0025\x005000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CBOT,QBALGO\x00100\x0011160400\x00Corn Futures\x00\x00202403\x00\x00\x00\x00US/Central\x0020231205:1900-20231206:0745;20231206:0830-20231206:1320;20231206:1900-20231207:0745;20231207:0830-20231207:1320;20231207:1900-20231208:0745;20231208:0830-20231208:1320;20231209:CLOSED;20231210:1900-20231211:0745;20231211:0830-20231211:1320;20231211:1900-20231212:0745;20231212:0830-20231212:1320\x0020231206:0830-20231206:1320;20231207:0830-20231207:1320;20231208:0830-20231208:1320;20231209:CLOSED;20231210:CLOSED;20231211:0830-20231211:1320;20231211:1900-20231212:0745;20231212:0830-20231212:1320\x00\x00\x000\x002147483647\x00ZC\x00IND\x00151,151\x0020240314\x00\x001\x001\x001\x00",
    )

    # Arrange
    instrument_id = InstrumentId.from_str("ZC-ZC=H24.CBOT")

    client = instrument_provider.client
    await client.connect()

    def send_messages(_):
        client._handle_msg(message)

    send_mock = Mock(side_effect=send_messages)
    client._conn.sendMsg = send_mock

    # Act
    instrument = await instrument_provider.load_contract(instrument_id)
    assert instrument is not None

    # send_mock.assert_called_once_with(
    #     b'\x00\x00\x00.9\x008\x00-10\x000\x00ZC\x00FUT\x00202403\x000.0\x00\x00\x00CBOT\x00\x00\x00\x00ZC\x000\x00\x00\x00\x00'
    # )


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


@pytest.mark.skip(reason="universe")
@pytest.mark.asyncio()
async def test_load_future_chain_details_universe(instrument_provider):
    for chain in IBTestProviderStubs.universe_future_chains():
        details_list = await instrument_provider.load_future_chain_details(chain)
        assert len(details_list) > 0
        await asyncio.sleep(1)

    # {1, 100}


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
