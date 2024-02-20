# fmt: off
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapters.interactive_brokers.parsing import contract_details_to_instrument
from pyfutures.adapters.interactive_brokers.parsing import instrument_id_to_contract

from pyfutures.continuous.chain import ContractChain
# from pyfutures.continuous.chain import ContractId
from pyfutures.continuous.contract_month import ContractMonth

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
        super().__init__(
            config=config,
        )

        self.client = client
        self.config = config
        self.contract_details: dict[str, IBContractDetails] = {}
        self.contract_id_to_instrument_id: dict[int, InstrumentId] = {}

        self._chain_filters = config.chain_filters or {}
        self._parsing_overrides = config.parsing_overrides
    
    async def load_contract(
        self,
        contract_id: InstrumentId,
    ) -> FuturesContract | None:
        
        """
        Expects InstrumentId to be in the format TradingClass-Symbol=ContractMonth.Exchange
        """
        
        if isinstance(contract_id, InstrumentId):
            contract: IBContract = instrument_id_to_contract(contract_id)
        
        return await self._load_contract(contract)
    
    async def _load_contract(
        self,
        contract: IBContract,
    ) -> FuturesContract | None:
        details_list = await self.client.request_contract_details(contract)

        if len(details_list) == 0:
            self._log.error("No contracts found")
            return None

        details_list = self._filter_monthly_contracts(details_list)

        futures_contract: FuturesContract = self.add_contract_details(details_list[0])
        
        return futures_contract
    
    async def load_futures_chain(
        self,
        chain: ContractChain,
    ) -> list[FuturesContract]:
        
        details_list = await self.request_future_chain_details(chain)

        futures_contracts: list[FuturesContract] = [
            self.add_contract_details(details) for details in details_list
        ]
        
        return futures_contracts
        
    async def request_future_chain_details(
        self,
        chain: ContractChain,
    ) -> list[IBContractDetails]:
        """
        Excepts a contract with TradingClass and Symbol properties set
        """
        
        contract: IBContract = instrument_id_to_contract(chain.instrument_id)
        
        contract.secType = "FUT"
        
        details_list = await self.client.request_contract_details(contract)

        if len(details_list) == 0:
            self._log.error("No contracts found")
            return []

        details_list = self._filter_monthly_contracts(details_list)

        return [
            details for details in details_list
            if ContractMonth.from_int(details.contractMonth) in chain.hold_cycle
        ]
        
    def _filter_monthly_contracts(
        self,
        details_list: list[IBContractDetails],
    ) -> list[IBContractDetails]:

        """
        For instruments that have weekly and monthly contracts in the same TradingClass it is
        required to apply a custom filter to ensure only monthly contracts are returned.
        """
        
        contract_months = [x.contractMonth for x in details_list]
        has_duplicates = len(contract_months) != len(set(contract_months))

        if not has_duplicates:
            return details_list

        tradingClass = details_list[0].contract.tradingClass

        filter_func = self._chain_filters.get(tradingClass)
        if filter_func is None:
            self._log.error(
                "Contract chain has duplicate contract months."
                " specify a monthly contract filter",
            )
            return []

        details_list = list(filter(filter_func, details_list))

        if len(details_list) > 1:
            self._log.error(
                "The chain filter func failed to return unique monthly contracts",
            )
            return []

        return details_list
    
    async def find_with_contract_id(self, contract_id: int) -> Instrument:
        instrument_id = self.contract_id_to_instrument_id.get(contract_id)
        if instrument_id is None:
            contract = IBContract()
            contract.conId = contract_id
            await self._load_contract(contract)
            instrument_id = self.contract_id_to_instrument_id.get(contract_id)
        instrument = self.find(instrument_id)
        return instrument
    
    def add_contract_details(self, details: IBContractDetails) -> Instrument:
        overrides: dict = self._parsing_overrides.get(details.contract.tradingClass, {})

        instrument = contract_details_to_instrument(details, overrides=overrides)
        self.add(instrument)
        self.contract_details[instrument.id.value] = details
        self.contract_id_to_instrument_id[details.contract.conId] = instrument.id
        return instrument

    async def load_ids_async(
            self,
            instrument_ids: list[InstrumentId],
            filters: dict | None = None,
        ) -> None:
        for id in instrument_ids:
            await self.load_contract(contract_id=id)


