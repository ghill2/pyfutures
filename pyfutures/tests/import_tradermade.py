import requests
from dotenv import dotenv_values
import yfinance as yf
import pandas as pd
import time
from pyfutures.tests.adapters.interactive_brokers.test_kit import TRADERMADE_FOLDER

TRADERMADE_SYMBOLS = [
    "GBPCAD",
    "GBPJPY",
    "GBPCHF",
    "EURGBP",
    "GBPUSD",
    "GBPAUD",
    "GBPCNH",
]

if __name__ == "__main__":
    api_key = dotenv_values()["tradermade_key"]

    days = pd.date_range(
        start=pd.Timestamp("1987-01-01", tz="UTC"),
        end=pd.Timestamp.utcnow() + pd.Timedelta(days=5),
        freq=pd.Timedelta(days=1),
    ).to_series()

    # for a date requested for Saturday or Sunday, it returns the Friday date.
    days = days[(days.dt.dayofweek != 5) & (days.dt.dayofweek != 6)]
    print(len(days))

    ticker_str = ",".join(TRADERMADE_SYMBOLS)

    # iterate
    grouped = days.groupby([days.dt.year, days.dt.month])
    for item in grouped:
        group, days = item
        year, month = group

        path = TRADERMADE_FOLDER / f"{year}_{month}.csv"

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

                timestamp = pd.Timestamp(data["date"])
                df.loc[len(df)] = (timestamp, *quote.values())

        path.parent.mkdir(exist_ok=True, parents=True)
        print(f"Writing {str(path)}...")
        df.to_csv(path, index=False)


# start_years = {
#     "GBPCAD": 1987,
#     "GBPJPY": 1987,
#     "GBPCHF": 1987,
#     "EURGBP": 1989,
#     "GBPUSD": 1987,
#     "GBPAUD": 1987,
#     "GBPCNH": 2014,
# }

# # get available tickers
# url = f"https://marketdata.tradermade.com/api/v1/historical_currencies_list?api_key={api_key}"
# resp = requests.get(url)
# tickers = resp.json()["available_currencies"]
# tickers = [
#     x for x in tickers if x.endswith("GBP") or x.startswith("GBP")
# ]
# ticker_str = ','.join(tickers)
# print(ticker_str)
