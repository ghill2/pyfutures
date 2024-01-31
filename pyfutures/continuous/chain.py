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
        
        self.current_bar_type = None  # initialized on start
        self.forward_bar_type = None  # initialized on start
        self.carry_bar_type = None  # initialized on start
        self.current_contract = None  # initialized on start
        self.forward_contract = None  # initialized on start
        self.carry_contract = None  # initialized on start
        self.hold_cycle = config.hold_cycle
        
        self._roll_offset = config.roll_offset
        self._instrument_id = InstrumentId.from_str(str(config.instrument_id))
        self._carry_offset = config.carry_offset
        self._priced_cycle = config.priced_cycle
        self._instrument_provider = instrument_provider
        
        assert self._roll_offset <= 0
        assert self._carry_offset == 1 or self._carry_offset == -1
        
    @property
    def instrument_provider(self) -> InstrumentProvider:
        return self._instrument_provider
    
    def roll(self) -> None:
        
        assert self.current_contract is not None
        self.current_contract = self._forward_contract(self.current_contract)
        self.forward_contract = self.forward_contract(self.current_contract)
        self.carry_contract = self.carry_contract(self.current_contract)
        self.expiry_date = unix_nanos_to_dt(self.current_contract.expiration_ns)
        self.expiry_day = self.expiry_date.floor("D")
        self.roll_date = self._roll_date(self.current_contract)
    
    def _roll_date(self, contract: FuturesContract) -> pd.Timestamp:
        # TODO: factor in the calendar
        return unix_nanos_to_dt(contract.expiration_ns) + pd.Timedelta(days=self._roll_offset)
            
    def _current_contract(self, month: ContractMonth) -> FuturesContract:
        month = self.current_month(month)
        self._instrument_provider.load_contract(
                instrument_id=self._instrument_id,
                month=month,
        )
        return self._instrument_provider.get_contract(self._instrument_id, month)
    
    def _forward_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.forward_month(current.info["month"])
        self._instrument_provider.load_contract(
                instrument_id=self._instrument_id,
                month=month,
        )
        return self._instrument_provider.get_contract(self._instrument_id, month)
        
    def _carry_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.carry_month(current.info["month"])
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