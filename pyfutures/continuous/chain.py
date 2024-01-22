from dataclasses import dataclass

import pandas as pd

from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.common.providers import InstrumentProvider
from copy import deepcopy

class MockInstrumentProvider(InstrumentProvider):
    def __init__(
        self,
        config: FuturesChainConfig,
        base: FuturesContract,
    ):
        
        self._approximate_expiry_offset = config.approximate_expiry_offset
        self._roll_offset = config.roll_offset
        assert self._roll_offset <= 0
        self._base = base
        
    def load(self, instrument_id: InstrumentId) -> FuturesContract:
        month = ContractMonth(instrument_id.symbol.value.split("[")[1].split("]")[0])
        
        approximate_expiry_date =  month.timestamp_utc \
            + pd.Timedelta(days=self._approximate_expiry_offset)
        
        roll_date = approximate_expiry_date(month) \
            + pd.Timedelta(days=self._roll_offset)
            
        futures_contract = FuturesContract(
            instrument_id=self._base.instrument_id,
            raw_symbol=self._base.raw_symbol,
            asset_class=self._base.asset_class,
            currency=self._base.currency,
            price_precision=self._base.price_precision,
            price_increment=self._base.price_increment
            multiplier=self._base.multiplier,
            lot_size=self._base.lot_size,
            underlying=self._base.underlying,
            activation_ns=0,
            expiration_ns=dt_to_unix_nanos(approximate_expiry_date),
            ts_event=0,
            ts_init=0,
        )
        futures_contract.month = month
        futures_contract.roll_date_ns = dt_to_unix_nanos(roll_date)

class FuturesContractChain:
    def __init__(
        self,
        config: FuturesChainConfig,
        instrument_provider: InstrumentProvider,
    ):
        
        self.instrument_id = InstrumentId.from_str(str(config.instrument_id))
        skip_months = list(map(ContractMonth, config.skip_months))
        self.hold_cycle = RollCycle.from_str(config.hold_cycle, skip_months=skip_months)
        self.priced_cycle = RollCycle(config.priced_cycle)
        self.carry_offset = config.carry_offset
        
        assert self.carry_offset == 1 or self.carry_offset == -1
        
        self._instrument_provider = instrument_provider
        
    def current_contract(self, timestamp: pd.Timestamp) -> FuturesContract:
        month = self.current_month(timestamp)
        instrument_id = self._fmt_instrument_id(month=month)
        futures_contract = self._instrument_provider.load(instrument_id)
        return futures_contract
    
    def forward_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.forward_month(current.month)
        instrument_id = self._fmt_instrument_id(month=month)
        futures_contract = self._instrument_provider.load(instrument_id)
        return futures_contract
        
    def carry_contract(self, current: FuturesContract) -> FuturesContract:
        month = self.carry_month(current.month)
        instrument_id = self._fmt_instrument_id(month=month)
        futures_contract = self._instrument_provider.load(instrument_id)
        return futures_contract
    
    def current_month(self, timestamp: pd.Timestamp) -> ContractMonth:
        
        current = self.hold_cycle.current_month(timestamp)

        while True:
            roll_date = self.roll_date(current)

            if roll_date > timestamp:
                break

            current = self.hold_cycle.next_month(current)

        return current
    
    def forward_month(self, month: ContractMonth) -> ContractMonth:
        return self.hold_cycle.next_month(month)

    def carry_month(self, month: ContractMonth) -> ContractMonth:
        if self.carry_offset == 1:
            return self.priced_cycle.next_month(month)
        elif self.carry_offset == -1:
            return self.priced_cycle.previous_month(month)
        else:
            raise ValueError("carry offset must be 1 or -1")
    
    def _fmt_instrument_id(self, month: ContractMonth) -> InstrumentId:
        """
        Return the InstrumentId of a specific month.
        symbol = f"{m['symbol']}{m['month']}{decade_digit(m['year'], contract)}{m['year']}"
        venue = contract.exchange
        """
        symbol = self.instrument_id.symbol.value
        venue = self.instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}[{month.year}{month.letter_month}].{venue}",
        )
        
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