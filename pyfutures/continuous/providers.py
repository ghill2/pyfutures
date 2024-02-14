from dataclasses import dataclass

import pandas as pd

from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.common.providers import InstrumentProvider
from pyfutures.continuous.config import NonPositiveInt

from nautilus_trader.config import PositiveInt

class TestContractProvider(InstrumentProvider):
    def __init__(
        self,
        approximate_expiry_offset: int,
        base: FuturesContract,
    ):
        super().__init__()
        self._approximate_expiry_offset = approximate_expiry_offset
        self._base = base
        self._contracts: dict[str, FuturesContract] = {}
    
    async def load_async(
        self,
        instrument_id: InstrumentId,
        filters: dict | None = None,
    ) -> None:
        
        parts = instrument_id.symbol.value.split("=")
        month = ContractMonth(parts[1])
        instrument_id = InstrumentId.from_str(
            f"{parts[0]}.{instrument_id.venue}"
        )
        return self.load_contract(instrument_id, month)
        
            
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
            info={
                "month": month,
            }
        )
        
        self.add(futures_contract)
        return futures_contract
        
    def get_contract(self, instrument_id: InstrumentId, month: ContractMonth) -> None:
        instrument_id = self._fmt_instrument_id(self._base.id, month)
        return self.find(instrument_id)
    
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