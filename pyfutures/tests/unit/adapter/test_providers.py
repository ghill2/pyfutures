# from pyfutures.adapter..client.objects import IBFuturesInstrument
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from ibapi.contract import ContractDetails as IBContractDetails
from nautilus_trader.adapters.interactive_brokers.client import InteractiveBrokersClient
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price

from pyfutures.continuous.cycle import RollCycle
from pyfutures.tests.unit.adapter.stubs import AdapterStubs
from pyfutures.tests.unit.client.stubs import ClientStubs


class TestInteractiveBrokersInstrumentProvider:
    def setup_method(self):
        client: InteractiveBrokersClient = ClientStubs.client()
        self.provider = AdapterStubs.instrument_provider(client=client)

    @pytest.mark.asyncio()
    async def test_load_async_returns_expected(self):
        # Arrange
        details = IBContractDetails()
        mock_contract = AdapterStubs.contract()

        self.provider._details_to_instrument = Mock(return_value=mock_contract)
        self.provider.client.request_contract_details = AsyncMock(return_value=[details])

        # Act
        await self.provider.load_async(mock_contract.id)

        # Assert
        assert self.provider.find(mock_contract.id) is not None
        assert self.provider.find(mock_contract.id) == mock_contract
        assert self.provider.contract_id_to_instrument_id[1] == mock_contract.id

    @pytest.mark.asyncio()
    async def test_parsing_overrides(self):
        # Arrange
        details = IBContractDetails()
        details.contract.tradingClass = "MIX"
        details.contract.secType = "FUT"
        request_mock = AsyncMock(return_value=[details])
        self.provider.client.request_contract_details = request_mock

        mock_contract = AdapterStubs.contract(
            instrument_id=InstrumentId.from_str("IBEX-MIX=FUT=2024F.MEFFRV"),
        )
        parsing_mock = Mock(return_value=mock_contract)
        self.provider._parser.details_to_instrument = parsing_mock

        # Act
        await self.provider.load_async(mock_contract.id)

        # Assert
        parsing_mock.assert_called_once_with(
            details=details,
            overrides={
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        )

    @pytest.mark.asyncio()
    async def test_load_async_uses_chain_filter(self):
        # Arrange
        details1 = IBContractDetails()
        details2 = IBContractDetails()

        self.provider.client.request_contract_details = AsyncMock(
            return_value=[details1, details2],
        )
        self.provider._filter_monthly_contracts = Mock(return_value=[details1])

        instrument_id = InstrumentId.from_str("M7EU=FMEU=FUT=2024H.EUREX")
        mock_contract = AdapterStubs.contract(instrument_id)
        self.provider._parser.details_to_instrument = Mock(return_value=mock_contract)

        # Act
        await self.provider.load_async(instrument_id)

        # Assert
        self.provider._filter_monthly_contracts.assert_called_once_with([details1, details2])

    @pytest.mark.asyncio()
    async def test_filter_monthly_contracts(self):
        # Arrange
        details1 = IBContractDetails()
        details1.contract.tradingClass = "FMEU"
        details1.contract.localSymbol = "X"

        details2 = IBContractDetails()
        details2.contract.tradingClass = "FMEU"
        details2.contract.localSymbol = "D"

        # Act
        filtered = self.provider._filter_monthly_contracts([details1, details2])

        # Assert
        assert filtered == [details1]

    @pytest.mark.asyncio()
    async def test_load_async_filters_monthly_contracts(self):
        # Arrange
        details1 = IBContractDetails()
        details1.contract.tradingClass = "FMEU"
        details1.contract.localSymbol = "month"  # monthly contract

        details2 = IBContractDetails()
        details2.contract.tradingClass = "FMEU"
        details2.contract.localSymbol = "D"  # daily contract

        self.provider.client.request_contract_details = AsyncMock(
            return_value=[details1, details2],
        )

        instrument_id = InstrumentId.from_str("M7EU=FMEU=FUT=2024H.EUREX")
        mock_contract = AdapterStubs.contract(instrument_id)

        self.provider._parser.details_to_instrument = Mock(return_value=mock_contract)

        # Act
        await self.provider.load_async(instrument_id)

        # Assert
        assert len(self.provider.list_all()) == 1

    @pytest.mark.asyncio()
    async def test_request_future_chain_filters_cycle_returns_expected(self):
        details1 = IBContractDetails()
        details1.contract.tradingClass = "FMEU"
        details1.contractMonth = "202401"

        details2 = IBContractDetails()
        details2.contract.tradingClass = "FMEU"
        details2.contractMonth = "202402"

        details3 = IBContractDetails()
        details3.contract.tradingClass = "FMEU"
        details3.contractMonth = "202403"

        self.provider.client.request_contract_details = AsyncMock(
            return_value=[details1, details2, details3],
        )

        details_list = await self.provider.request_future_chain(
            instrument_id=InstrumentId.from_str("M7EU=FMEU=FUT=2024H.EUREX"),
            cycle=RollCycle("FG"),
        )
        assert details_list == [details1, details2]

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_request_future_chain_filters_cycle_and_monthly_contracts(self):
        print("TEST")
        details1 = IBContractDetails()
        details1.contract.tradingClass = "FMEU"
        details1.contractMonth = "202401"
        details1.contract.localSymbol = "X"  # monthly contract

        details2 = IBContractDetails()
        details2.contract.tradingClass = "FMEU"
        details2.contractMonth = "202402"
        details2.contract.localSymbol = "D"  # daily contract

        details3 = IBContractDetails()
        details3.contract.tradingClass = "FMEU"
        details3.contractMonth = "202403"
        details2.contract.localSymbol = "X"  # monthly contract

        self.provider.client.request_contract_details = AsyncMock(
            return_value=[details1, details2, details3],
        )

        details_list = await self.provider.request_future_chain(
            instrument_id=InstrumentId.from_str("M7EU=FMEU=FUT=2024H.EUREX"),
            cycle=RollCycle("FG"),
        )
        assert details_list == [details1]

    # @pytest.mark.skip()
    # @pytest.mark.asyncio()
    # async def test_find_with_contract_id_requests_instrument(instrument_provider):
    #     await instrument_provider.client.connect()

    #     contract = await instrument_provider.find_with_contract_id(564400671)

    #     assert contract.id == InstrumentId.from_str("D-RC=F24.ICEEUSOFT")


# @pytest.mark.skip()
# @pytest.mark.asyncio()
# async def test_load_with_safe_instrument_id(instrument_provider):
#     # Arrange
#     instrument_id = InstrumentId.from_str("MNTPX-TPXM=H24.OSE|JPN")

#     # Act
#     instrument = await instrument_provider.load_contract(instrument_id)
#     assert instrument is not None

# @pytest.mark.asyncio()
# async def test_load_with_instrument_id(instrument_provider):
#     # Arrange
#     instrument_id = InstrumentId.from_str("ZC-ZC=H24.CBOT")

#     # Act
#     instrument = await instrument_provider.load_contract(instrument_id)
#     assert instrument is not None


# @pytest.mark.asyncio()
# async def test_load_with_safe_instrument_id(instrument_provider):
#     # Arrange
#     instrument_id = InstrumentId.from_str("MNTPX-TPXM=H24.OSE|JPN")

#     # Act
#     instrument = await instrument_provider.load_contract(instrument_id)
#     assert instrument is not None


# @pytest.mark.asyncio()
# async def test_load_uses_chain_filter(instrument_provider):
#     # Arrange
#     instrument_provider._chain_filters = {
#         "FMEU": lambda x: not x.contract.localSymbol.endswith("D"),
#     }
#     instrument_id = InstrumentId.from_str("M7EU-FMEU=H24.EUREX")

#     # Act
#     details = await instrument_provider.load_contract(instrument_id)

#     # Assert
#     assert details is not None


# @pytest.mark.asyncio()
# async def test_load_parsing_overrides_sets_expected(instrument_provider):
#     # Arrange
#     instrument_provider._parsing_overrides = {
#         "MIX": {
#             "price_precision": 0,
#             "price_increment": Price(5, 0),
#         },
#     }
#     instrument_id = InstrumentId.from_str("IBEX-MIX=F24.MEFFRV")

#     # Act
#     instrument = await instrument_provider.load_contract(instrument_id)

#     # Assert
#     assert instrument.price_precision == 0
#     assert instrument.price_increment == 5


# @pytest.mark.asyncio()
# async def test_request_future_chain_details_returns_expected(instrument_provider):
#     config = FuturesChainConfig(
#         instrument_id="ZN-ZN.CBOT",
#         hold_cycle="HMUZ",
#         priced_cycle="HMUZ",
#         roll_offset=-25,
#         approximate_expiry_offset=19,
#         carry_offset=1,
#     )
#     chain = FuturesChain(config=config)

#     details_list = await instrument_provider.request_future_chain_details(chain)
#     for details in details_list:
#         assert ContractMonth.from_int(details.contractMonth) in chain.hold_cycle


# @pytest.mark.asyncio()
# async def test_find_with_contract_id_requests_instrument(instrument_provider):
#     await instrument_provider.client.connect()

#     contract = await instrument_provider.find_with_contract_id(564400671)

#     assert contract.id == InstrumentId.from_str("D-RC=F24.ICEEUSOFT")


######################################################################################


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
