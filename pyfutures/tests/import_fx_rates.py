import requests
from dotenv import dotenv_values
import yfinance as yf
import pandas as pd
import time
from pyfutures.tests.adapters.interactive_brokers.test_kit import FX_RATES_FOLDER

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

        df = msft.history(period="max", interval="1d")
        time.sleep(5)
        
        path = FX_RATES_FOLDER / f"{ticker}.csv"
        path.parent.mkdir(exist_ok=True, parents=True)
        print(f"Writing {str(path)}...")
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
        #     [["timestamp", "open", "high", "low", "close", "volume"]]
        df.reset_index().to_csv(path)
        

def export_tradermade():
    
    """
    close        date     high      low     open
0    1.14645  2019-01-01  1.14692  1.14083  1.14657
1    1.13444  2019-01-02  1.14971  1.13251  1.14645

    1987-01-03 5 Saturd {'base_currency': 'GBP', 'close': 2.05268, 'high': 2.06573, 'low': 1.4827, 'open': 1.484, 'quote_currency': 'CAD'}
    1987-01-04 6 Sunday {'base_currency': 'GBP', 'close': 2.05268, 'high': 2.06573, 'low': 1.4827, 'open': 1.484, 'quote_currency': 'CAD'}
    
    1987-01-10 5 Saturd {'base_currency': 'GBP', 'close': 2.02509, 'high': 2.02688, 'low': 1.4701, 'open': 1.4767, 'quote_currency': 'CAD'}
    1987-01-11 6 Sunday {'base_currency': 'GBP', 'close': 2.02509, 'high': 2.02688, 'low': 1.4701, 'open': 1.4767, 'quote_currency': 'CAD'}
    
    1987-01-17 5 Saturd {'base_currency': 'GBP', 'close': 2.06751, 'high': 2.07309, 'low': 1.5015, 'open': 1.504, 'quote_currency': 'CAD'}
    1987-01-18 6 Sunday {'base_currency': 'GBP', 'close': 2.06751, 'high': 2.07309, 'low': 1.5015, 'open': 1.504, 'quote_currency': 'CAD'}
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
    
    print(pd.Timestamp("1987-01-17", tz="UTC").dayofweek)
    
    """
    0 = Monday
    1 = Tuesday
    2 = Weds
    3 = Thurs
    4 = Friday
    5 = Saturday
    6 = Sunday
    """
    
    # create days
    days = pd.date_range(
        start=pd.Timestamp("1987-01-01", tz="UTC"),
        end=pd.Timestamp.utcnow() + pd.Timedelta(days=5),
        freq=pd.Timedelta(days=1),
    ).to_series()
    
    # for a date requested for Saturday or Sunday, it returns the Friday date.
    days = days[(days.dt.dayofweek != 5) & (days.dt.dayofweek != 6)]
    print(len(days))
    
    # # get available tickers
    # url = f"https://marketdata.tradermade.com/api/v1/historical_currencies_list?api_key={api_key}"
    # resp = requests.get(url)
    # tickers = resp.json()["available_currencies"]
    # tickers = [
    #     x for x in tickers if x.endswith("GBP") or x.startswith("GBP")
    # ]
    # ticker_str = ','.join(tickers)
    # print(ticker_str)
    ticker_str = "GBPCAD,GBPJPY,GBPCHF,EURGBP,GBPUSD,GBPAUD,GBPCNH" 
    
    # iterate
    grouped = days.groupby([days.dt.year, days.dt.month])
    for item in grouped:
        group, days = item
        year, month = group
        
        path = FX_RATES_FOLDER / f"{year}_{month}.csv"
        
        if path.exists():
            print(f"Path exists {path}")
            continue
        
        df = pd.DataFrame(
            columns=["timestamp", "base_currency", "close", "high", "low", "open", "quote_currency"],
        )
            
        for i, day in enumerate(days):
            
            start = time.perf_counter()
            
            day_str = day.strftime("%Y-%m-%d")
            url = f"https://marketdata.tradermade.com/api/v1/historical?currency={ticker_str}&date={day_str}&api_key={api_key}"
            resp = requests.get(url)
            
            stop = time.perf_counter()
            elapsed = stop - start

            print(f"{year}_{month}: {i}/{len(days)} {elapsed:.2f}s")
            
            data = resp.json()
            for quote in data["quotes"]:
                
                if "error" in quote:  # and quote["error"] == 204
                    continue
                
                timestamp = pd.Timestamp(data['date'])
                df.loc[len(df)] = (timestamp, *quote.values())
        
        
        path.parent.mkdir(exist_ok=True, parents=True)
        print(f"Writing {str(path)}...")
        df.to_csv(path, index=False)
    
if __name__ == "__main__":
    export_tradermade()