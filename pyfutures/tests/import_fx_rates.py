import requests
from dotenv import dotenv_values
import yfinance as yf
import pandas as pd
import time
from pyfutures.tests.adapters.interactive_brokers.test_kit import FX_RATES_FOLDER
from pyfutures.data.files import ParquetFile
from pyfutures.data.writer import QuoteTickParquetWriter
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import InstrumentId
from pathlib import Path
from pyfutures.tests.import_tradermade import TRADERMADE_SYMBOLS


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
    
    price_precision = PRICE_PRECISIONS[symbol]
    
    if str(symbol).startswith("GBP"):
        df.bid_price = 1 / df.bid_price
        df.ask_price = 1 / df.ask_price
        symbol = f"{symbol[3:6]}{symbol[:3]}"
    
    instrument_id = InstrumentId.from_str(f"{symbol}.SIM")
    bar_type = BarType.from_str(
        f"{instrument_id}.SIM-1-DAY-MID-EXTERNAL",
    )
    
    file = ParquetFile(
        parent=FX_RATES_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )
    file.path.parent.mkdir(exist_ok=True, parents=True)
    writer = QuoteTickParquetWriter(
        instrument_id=instrument_id,
        path=file.path,
        price_precision=price_precision,
        size_precision=1,
    )
    print(f"Writing {str(file.path)}...")
    writer.write_dataframe(df)

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
        )
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
        df = pd.DataFrame(
            {
                "timestamp": df.index.tz_convert("UTC"),
                "bid_price": df.Close,
                "ask_price": df.Close,
                "bid_size": 1.0,
                "ask_size": 1.0,
            }
        )
        print(ticker)
        print(df)
        
        write_dataframe(
            symbol=ticker.replace('=X', ''),
            df=df,
        )
        time.sleep(5)
        
if __name__ == "__main__":
    # import_tradermade()
    import_yahoo()
    
    
    
    
    
    # df = df \
        #     .reset_index() \
        #     .rename(
        #         {
        #             "Open":"open",
        #             "High":"high",
        #             "Low":"low",
        #             "Close":"close",
        #             "Volume":"volume",
        #             "Date":"timestamp",
        #         },
        #     axis=1) \
        #     [["timestamp", ]]