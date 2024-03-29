from decimal import Decimal

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity

from pyfutures.tests.test_kit import IBTestProviderStubs


class UniverseInstrumentProvider(InstrumentProvider):
    """
    Provides instruments to the backtest engine
    """

    def __init__(self):
        super().__init__(
            logger=Logger(
                clock=LiveClock(),
                level_stdout=LogLevel.INFO,
                # bypass=True,
            ),
        )

        universe = IBTestProviderStubs.universe_dataframe()

        for row in universe.itertuples():
            self.add(
                Instrument(
                    instrument_id=InstrumentId.from_str(
                        f"{row.trading_class}_{row.symbol}.IB"
                    ),
                    raw_symbol=Symbol(row.symbol),
                    asset_class=AssetClass.COMMODITY,
                    asset_type=InstrumentClass.FUTURE,
                    quote_currency=Currency.from_str(
                        row.quote_currency.split("(")[1].split(")")[0]
                    ),
                    is_inverse=False,
                    price_precision=IBTestProviderStubs.price_precision(
                        min_tick=row.min_tick,
                        price_magnifier=row.price_magnifier,
                    ),
                    size_precision=0,
                    size_increment=Quantity.from_int(1),
                    multiplier=Quantity.from_str(
                        str(row.multiplier * row.price_magnifier)
                    ),
                    margin_init=Decimal(),
                    margin_maint=Decimal(),
                    maker_fee=Decimal(),
                    taker_fee=Decimal(),
                    ts_event=0,
                    ts_init=0,
                    price_increment=IBTestProviderStubs.price_increment(
                        min_tick=row.min_tick,
                        price_magnifier=row.price_magnifier,
                    ),
                    lot_size=Quantity.from_int(1),
                    max_quantity=None,
                    min_quantity=None,
                    max_notional=None,
                    min_notional=None,
                    max_price=None,
                    min_price=None,
                    tick_scheme_name=None,
                    info=None,
                )
            )


if __name__ == "__main__":
    UniverseInstrumentProvider()
