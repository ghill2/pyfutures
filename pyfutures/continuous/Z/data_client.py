from datetime import datetime

import pandas as pd
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.futures_contract import FuturesContract

from pyfutures.continuous.contract_month import ContractMonth


class ContractClient(BacktestDataClient):
    def __init__(
        self,
        approximate_expiry_offset: int,
        base: FuturesContract,
    ):
        self._approximate_expiry_offset = approximate_expiry_offset
        self._base = base

    def request_instrument(
        self,
        instrument_id: InstrumentId,
        correlation_id: UUID4,
        start: datetime = None,
        end: datetime = None,
    ):
        month = ContractMonth(instrument_id.symbol.value.split("=")[1])
        approximate_expiry_date = month.timestamp_utc + pd.Timedelta(days=self._approximate_expiry_offset)

        instrument_id = self._fmt_instrument_id(self._base.id, month)
        contract = FuturesContract(
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
            },
        )
        self._handle_instrument(contract, correlation_id)

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
