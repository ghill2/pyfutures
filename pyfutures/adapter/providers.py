from collections.abc import Callable

from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.instruments.futures_contract import FuturesContract

from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.parsing import AdapterParser
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


class InteractiveBrokersInstrumentProvider(InstrumentProvider):
    """
    Provides a means of loading `Instrument` objects through Interactive Brokers.
    """

    def __init__(
        self,
        client: InteractiveBrokersClient,
        config: InteractiveBrokersInstrumentProviderConfig = None,
    ):
        config = config or InteractiveBrokersInstrumentProviderConfig()
        super().__init__(config=config)

        self.client = client
        self.config = config
        # self.contract_details: dict[InstrumentId, IBContractDetails] = {}
        self.contract_id_to_instrument_id: dict[int, InstrumentId] = {}

        self._chain_filters = config.chain_filters or {}
        self._parsing_overrides = config.parsing_overrides or {}
        self._parser = AdapterParser()

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        for instrument_id in instrument_ids:
            await self.load_async(instrument_id)

    async def load_async(
        self,
        instrument_id: InstrumentId | IBContract,
    ) -> None:
        contract: IBContract = self._parse_input(instrument_id)

        details_list = await self._request_details(contract)

        if len(details_list) == 0:
            return None

        assert len(details_list) == 1

        instrument = self._details_to_instrument(details_list[0])
        self._add_instrument(instrument)

    async def load_futures_chain(
        self,
        instrument_id: InstrumentId | IBContract,
        cycle: RollCycle | None = None,
    ) -> list[FuturesContract]:
        details_list = await self.request_future_chain(
            instrument_id=instrument_id,
            cycle=cycle,
        )

        contracts: list[FuturesContract] = [self._details_to_instrument(d) for d in details_list]

        for c in contracts:
            self._add_instrument(c)

        return contracts

    async def request_future_chain(
        self,
        instrument_id: InstrumentId | IBContract,
        cycle: RollCycle | None = None,
    ) -> list[IBContractDetails]:
        contract: IBContract = self._parse_input(instrument_id)

        return [details for details in (await self._request_details(contract)) if ContractMonth.from_int(details.contractMonth) in cycle]

    async def _request_details(self, contract: IBContract) -> list[IBContractDetails]:
        details_list = await self.client.request_contract_details(contract)

        """
        Filtering monthly contracts:
        For instruments that have weekly and monthly contracts in the same TradingClass it is
        required to apply a custom filter to ensure only monthly contracts are returned.
        """

        if len(details_list) > 0 and contract.secType == "FUT":
            filter_func = self._chain_filters.get(contract.tradingClass)
            if filter_func is not None:
                details_list = list(filter(filter_func, details_list))

        self._assert_monthly_contracts(details_list)

        return details_list

    @staticmethod
    def _assert_monthly_contracts(details_list: list[IBContractDetails]) -> None:
        contract_months = [x.contractMonth for x in details_list]
        has_duplicates = len(contract_months) != len(set(contract_months))
        if has_duplicates:
            tradingClass = details_list[0].contract.tradingClass
            raise ValueError(
                f"Contract chain for trading class {tradingClass} contains weekly or daily contracts. " "Check or specify a monthly contract filter"
            )

    def _details_to_instrument(self, details: IBContractDetails) -> Instrument:
        instrument = self._parser.details_to_instrument(
            details=details,
            overrides=self._parsing_overrides.get(details.contract.tradingClass, {}),
        )
        return instrument

    def _add_instrument(self, instrument: Instrument) -> None:
        self.add(instrument)

        conId = instrument.info["contract"]["conId"]
        self.contract_id_to_instrument_id[conId] = instrument.id

    def _parse_input(self, value: InstrumentId | IBContract) -> IBContract:
        assert isinstance(value, (InstrumentId, IBContract))
        if isinstance(value, InstrumentId):
            return self._parser.instrument_id_to_contract(value)
        elif isinstance(value, IBContract):
            return value

    def _filter_monthly_contracts(
        self,
        details_list: list[IBContractDetails],
        filter_func: Callable,
    ) -> list[IBContractDetails]:
        return details_list

    # async def find_with_contract_id(self, contract_id: int) -> Instrument:
    #     instrument_id = self.contract_id_to_instrument_id.get(contract_id)
    #     if instrument_id is None:
    #         contract = IBContract()
    #         contract.conId = contract_id
    #         await self.load_contract(contract)
    #         instrument_id = self.contract_id_to_instrument_id.get(contract_id)
    #     instrument = self.find(instrument_id)
    #     return instrument


# contract_months = [x.contractMonth for x in details_list]
# has_duplicates = len(contract_months) != len(set(contract_months))

# if not has_duplicates:
#     return details_list
