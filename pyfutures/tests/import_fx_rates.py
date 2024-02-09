import requests
from dotenv import dotenv_values
import yfinance as yf
import pandas as pd
import time


def reduce_dataframe(
    
    is_inverse: bool,
) -> pd.DataFrame:
    pass

def export_yahoo():
    data = [
        ("INRGBP=X", False),
        ("HKDGBP=X", False),
        ("GBPKRW=X", True),
        ("GBPSGD=X", True),
    ]
    for item in data:
        ticker, is_inverse = item
        
        msft = yf.Ticker(ticker)
        print(msft.info)

        hist = msft.history(period="max", interval="1d")
        print(hist)
        time.sleep(5)

def export_tradermade():
    
    """
    close        date     high      low     open
0    1.14645  2019-01-01  1.14692  1.14083  1.14657
1    1.13444  2019-01-02  1.14971  1.13251  1.14645
    """
    
    api_key = dotenv_values()["tradermade_key"]
    
    start_years = {
        "GBPCAD": 1987,
        "GBPJPY": 1987,
        "GBPCHF": 1987,
        "EURGBP": 1989,
        "GBPUSD": 1987,
        "GBPAUD": 1987,
        "GBPCNH": 2014,
    }
    
    url = f"https://marketdata.tradermade.com/api/v1/pandasDF?currency=EURUSD&api_key={api_key}&start_date=2019-01-01&end_date=2020-01-01&format=records&fields=ohlc"
    resp = requests.get(url)
    df = pd.DataFrame(resp.json())
    
if __name__ == "__main__":
    
    