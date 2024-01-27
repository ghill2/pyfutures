from dataclasses import dataclass

import pandas as pd

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.common.providers import InstrumentProvider
from copy import deepcopy
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.config import NonPositiveInt

from nautilus_trader.config.validation import PositiveInt
    
class TestContractProvider(InstrumentProvider):
    def __init__(
        self,
        approximate_expiry_offset: int,
        base: FuturesContract,
    ):
        
        self._approximate_expiry_offset = approximate_expiry_offset
        self._base = base
        self._contracts: dict[str, FuturesContract] = {}
        
    def load_contract(self, instrument_id: InstrumentId, month: ContractMonth) -> None:
        
        approximate_expiry_date = month.timestamp_utc \
            + pd.Timedelta(days=self._approximate_expiry_offset)
        
        instrument_id = self._fmt_instrument_id(self._base.id, month)
        futures_contract = FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=self._base.raw_symbol,
            asset_class=self._base.asset_class,
            currency=self._base.quote_currency,
            price_precision=self._base.price_precision,
            price_increment=self._base.price_increment,
            multiplier=self._base.multiplier,
            lot_size=self._base.lot_size,
            underlying=self._base.underlying,
            activation_ns=0,
            expiration_ns=dt_to_unix_nanos(approximate_expiry_date),
            ts_event=0,
            ts_init=0,
        )
        futures_contract.month = month  # cdef readonly object month
        
        self._contracts[instrument_id.value] = futures_contract
        
    def get_contract(self, instrument_id: InstrumentId, month: ContractMonth) -> None:
        instrument_id = self._fmt_instrument_id(self._base.id, month)
        return self._contracts.get(instrument_id.value)
    
    @staticmethod
    def _fmt_instrument_id(instrument_id: InstrumentId, month: ContractMonth) -> InstrumentId:
        """
        Format the InstrumentId for contract given the ContractMonth.
        """
        symbol = instrument_id.symbol.value
        venue = instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}={month.year}{month.letter_month}.{venue}",
        )

@dataclass
class ContractDetails:
    instrument_id: InstrumentId
    month: ContractMonth
    expiry_date_ns : PositiveInt
    roll_date_ns: PositiveInt
    
class ContractChain:
    def __init__(
        self,
        config: ContractChainConfig,
        instrument_provider: InstrumentProvider,
    ):
        
        self.hold_cycle = config.hold_cycle
        
        self._roll_offset = config.roll_offset
        assert self._roll_offset <= 0
        self._instrument_id = InstrumentId.from_str(str(config.instrument_id))
        
        self._carry_offset = config.carry_offset
        
        self._priced_cycle = config.priced_cycle
        
        assert self._carry_offset == 1 or self._carry_offset == -1
        
        self._instrument_provider = instrument_provider
    
    @property
    def instrument_provider(self) -> InstrumentProvider:
        return self._instrument_provider
    
    def roll_date_utc(self, contract: FuturesContract) -> pd.Timestamp:
        return self._roll_date_utc(
                    expiry_date=unix_nanos_to_dt(contract.expiration_ns),
                    offset=self._roll_offset,
        )
                
    @staticmethod
    def _roll_date_utc(expiry_date: pd.Timestamp, offset: NonPositiveInt):
        # TODO: factor in the calendar
        return expiry_date + pd.Timedelta(days=offset)
    
    def current_contract(self, month: ContractMonth) -> FuturesContract:
        month = self.current_month(month)
        self._instrument_provider.load_contract(
                instrument_id=self._instrument_id,
                month=month,
        )
        return self._instrument_provider.get_contract(self._instrument_id, month)
    
    def forward_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.forward_month(current.month)
        self._instrument_provider.load_contract(
                instrument_id=self._instrument_id,
                month=month,
        )
        return self._instrument_provider.get_contract(self._instrument_id, month)
        
    def carry_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.carry_month(current.month)
        self._instrument_provider.load_contract(
                instrument_id=self._instrument_id,
                month=month,
        )
        return self._instrument_provider.get_contract(self._instrument_id, month)
        
    def current_month_from_timestamp(self, timestamp: pd.Timestamp) -> ContractMonth:
        
        current = self.hold_cycle.current_month(timestamp)
        
        while True:
            roll_date = self._roll_date_utc(
                            expiry_date=current.timestamp_utc,
                            offset=self._roll_offset,
                        )

            if roll_date > timestamp:
                break

            current = self.hold_cycle.next_month(current)

        return current
        
    def current_month(self, month: ContractMonth) -> ContractMonth:
        if month in self.hold_cycle:
            return month
        
        return self.forward_month(month)
    
    def forward_month(self, month: ContractMonth) -> ContractMonth:
        return self.hold_cycle.next_month(month)

    def carry_month(self, month: ContractMonth) -> ContractMonth:
        if self._carry_offset == 1:
            return self._priced_cycle.next_month(month)
        elif self._carry_offset == -1:
            return self._priced_cycle.previous_month(month)
        else:
            raise ValueError("carry offset must be 1 or -1")
    
    
        
    # def make_id(self, month: ContractMonth) -> ContractId:
    #     return ContractId(
    #         instrument_id=self._fmt_instrument_id(self.instrument_id, month),
    #         month=month,
    #     )
        
    # def carry_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.carry_month(current.month))

    # def forward_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.forward_month(current.month))

    # def current_id(self, timestamp: pd.Timestamp) -> ContractId:
    #     return self.make_id(self.current_month(timestamp))
        
    # def carry_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.carry_month(current.month))

    # def forward_id(self, current: ContractId) -> ContractId:
    #     return self.make_id(self.forward_month(current.month))

    # def current_id(self, timestamp: pd.Timestamp) -> ContractId:
    #     return self.make_id(self.current_month(timestamp))
    
        # f"{base.symbol.value}={month}.{base.venue.value}",

    
    
    # def make_id(self, month: ContractMonth) -> ContractId:
    #     return ContractId(
    #         instrument_id=self._fmt_instrument_id(self.instrument_id, month),
    #         month=month,
    #         approximate_expiry_date_utc=self.approximate_expiry_date(month),
    #         roll_date_utc=self.roll_date(month),
    #     )
    # @dataclass
# class ContractId:
#     instrument_id: InstrumentId
#     month: ContractMonth
#     approximate_expiry_date_utc: pd.Timestamp
#     roll_date_utc: pd.Timestamp