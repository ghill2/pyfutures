from collections.abc import Iterable
from pathlib import Path

import requests
from dotenv import dotenv_values


class FxRates:
    cachedir = Path.home() / "Desktop" / "pyfutures_cache" / "fx_rates"
    tradermade_key = dotenv_values()["TRADERMADE_KEY"]

    def get(self, currencies: Iterable):
        """
        Idea for caching:
         - as this method is called from stats and MarginInfo, currency list can be different
         - therefore, cache each key / currency separately and invalidate automatically if the cached entry is beyond a day old

        Example currencies argument:
            {'JPY', 'HKD', 'AUD', 'KRW', 'CAD', 'CNH', 'EUR', 'GBP', 'CHF', 'INR', 'USD', 'SGD'}
        """
        # today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
        # outpath = Path(self.cachedir / today / "fx_rates.json")
        # outpath.parent.mkdir(parents=True, exist_ok=True)
        #
        # if outpath.exists():
        #     print("-----> Getting FX Rates from file...")
        #     with open(outpath) as f:
        #         return json.load(f)
        # #
        print("-----> Getting FX Rates from API...")
        url = f"https://marketdata.tradermade.com/api/v1/live?currency={','.join(currencies)}&api_key={self.tradermade_key}"
        resp = requests.get(url)
        data = resp.json()

        fx_rates = {f"{d['base_currency']}{d['quote_currency']}": d["mid"] for d in data["quotes"]}

        # with open(outpath, "w") as f:
        #     json.dump(data, f, indent=4)

        return fx_rates

    # def find(self):
    #     """
    #         Find the fx rate in the response
    #     """
