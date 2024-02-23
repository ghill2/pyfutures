import time
from pathlib import Path

import pandas as pd
import yfinance as yf
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from pyfutures.data.files import ParquetFile
from nautilus_trader.model.identifiers import Venue
from pyfutures.data.writer import QuoteTickParquetWriter
from pyfutures.tests.adapters.interactive_brokers.test_kit import CATALOG_FOLDER
from pyfutures.tests.import_tradermade import TRADERMADE_SYMBOLS
from pyfutures.tests.adapters.interactive_brokers.test_kit import CATALOG
from nautilus_trader.model.instruments import CurrencyPair

PRICE_PRECISIONS = {
    "GBPCAD": 5,  # tradermade 1987
    "GBPJPY": 5,  # tradermade 1987
    "GBPCHF": 5,  # tradermade 1987
    "EURGBP": 5,  # tradermade 1989
    "GBPUSD": 5,  # tradermade 1987
    "GBPAUD": 5,  # tradermade 1987
    "GBPCNH": 4,  # tradermade 2013
    "INRGBP": 6,  # yahoo 2003
    "HKDGBP": 6,  # yahoo 2003
    "GBPKRW": 6,  # yahoo 2002
    "GBPSGD": 5,  # yahoo 2003
}


def write_dataframe(symbol: str, df: pd.DataFrame) -> None:
    # price_precision = PRICE_PRECISIONS[symbol]

    if str(symbol).startswith("GBP"):
        df.bid_price = 1 / df.bid_price
        df.ask_price = 1 / df.ask_price
        symbol = f"{symbol[3:6]}{symbol[:3]}"

    instrument: CurrencyPair = TestInstrumentProvider.default_fx_ccy(symbol=symbol, venue=Venue("SIM"))
    df.bid_price = df.bid_price.round(5)
    df.ask_price = df.ask_price.round(5)
    print(symbol, df)
    print(f"Writing {symbol!s}...")
    
    wrangler = QuoteTickDataWrangler(
        instrument=instrument,
    )
    quotes = wrangler.process(df)
    CATALOG.write_data(
        data=quotes,
        basename_template=instrument.id.value + "-{i}",
    )
    CATALOG.write_data(
        data=[instrument],
        basename_template=instrument.id.value + "-{i}",
    )


def import_tradermade() -> None:
    folder = Path("/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/tradermade/old")

    paths = folder.glob("*.csv")

    total = pd.DataFrame()
    for path in paths:
        df = pd.read_csv(path)
        total = pd.concat([total, df])

    for symbol in TRADERMADE_SYMBOLS:
        mask = (total.base_currency == symbol[:3]) & (total.quote_currency == symbol[3:6])
        df = total[mask].reset_index(drop=True)
        df.timestamp = pd.to_datetime(df.timestamp.values, utc=True)
        df.sort_values(by="timestamp", inplace=True)
        assert not df.empty

        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(df.timestamp.values, utc=True),
                "bid_price": df.close,
                "ask_price": df.close,
                "bid_size": 1.0,
                "ask_size": 1.0,
            }
        ).set_index("timestamp")
        write_dataframe(symbol=symbol, df=df)


def import_yahoo():
    tickers = [
        "INRGBP=X",
        "HKDGBP=X",
        "GBPKRW=X",
        "GBPSGD=X",
    ]
    for ticker in tickers:
        msft = yf.Ticker(ticker)
        df = msft.history(period="max", interval="1d")
        # print(ticker)
        # print(df)
        df = pd.DataFrame(
            {
                "timestamp": df.index.tz_convert("UTC"),
                "bid_price": df.Close,
                "ask_price": df.Close,
                "bid_size": 1.0,
                "ask_size": 1.0,
            }
        ).set_index("timestamp")
        write_dataframe(
            symbol=ticker.replace("=X", ""),
            df=df,
        )
        time.sleep(5)


if __name__ == "__main__":
    import_tradermade()
    import_yahoo()