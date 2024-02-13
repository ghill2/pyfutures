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
        
def import_yahoo():
    data = [
        ("INRGBP=X", False, 6),
        ("HKDGBP=X", False, 6),
        ("GBPKRW=X", True, 6),
        ("GBPSGD=X", True, 5),
    ]
    for item in data:
        ticker, reverse, price_precision = item
        
        msft = yf.Ticker(ticker)
        df = msft.history(period="max", interval="1d")
        
        if reverse:
            df.Close = 1 / df.Close
            ticker = f"{ticker[3:6]}{ticker[:3]}=X"
        
        symbol = Symbol(ticker.replace('=X', ''))
        
        df = pd.DataFrame(
            {
                "timestamp": df.index.tz_convert("UTC"),
                "bid_price": df.Close,
                "ask_price": df.Close,
                "bid_size": df.Volume.astype(float),
                "ask_size": df.Volume.astype(float),
            }
        )
        
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
        
        time.sleep(5)
        
if __name__ == "__main__":
    # import_yahoo()
    folder = Path("/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/tradermade/old")
    
    paths = folder.glob("*.csv")
    
    df = pd.DataFrame()
    for path in paths:
        df = pd.concat(
            [df, pd.read_csv(path)]
        )
    
    
    
    
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