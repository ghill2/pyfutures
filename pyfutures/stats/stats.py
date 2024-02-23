import asyncio
import json
import pickle
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values
from ibapi.common import Contract as IBContract
from nautilus_trader.common.component import Logger

from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.historic import InteractiveBrokersHistoric
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs as PyfuturesTestProviderStubs


logger = Logger(name="stats")
from bs4 import BeautifulSoup


TRADERMADE_KEY = dotenv_values()["tradermade_key"]


class Stats:
    def __init__(self, client: InteractiveBrokersClient, parent_out: str = None) -> None:
        """ """
        client._request_timeout_seconds = 60
        if parent_out is None:
            # "%Y-%m-%d_%H-%M-%S"
            self.parent_out = Path.home() / "Desktop" / "ib_data_download"

        yesterday = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        self.parent_out = self.parent_out / yesterday
        self.parent_out.mkdir(parents=True, exist_ok=True)
        self.client = client
        self.historic = InteractiveBrokersHistoric(client=client)

    async def _details(self, contract: IBContract):
        """
        gets contract_details from a pickle file in local folder
        or get from IB api if the pickle file does not exist
        """
        outpath = Path(self.parent_out / "details" / f"{contract.exchange}-{contract.symbol}.pickle")
        outpath.parent.mkdir(parents=True, exist_ok=True)
        if outpath.exists():
            print("-----> Getting Contract Details from file...")
            with open(outpath, "rb") as f:
                detail = pickle.load(f)
                return detail
        else:
            print("-----> Getting Contract Details from API...")

            await self.client.connect()
            detail = await self.client.request_front_contract_details(contract=contract)
            if detail is None:
                raise ValueError("No contract found for instrument...")
            with open(outpath, "wb") as f:
                pickle.dump(detail, f)
            return detail

    def _fx_rates(self, rows):
        outpath = Path(self.parent_out / "fx_rates.json")

        if outpath.exists():
            print("-----> Getting FX Rates from file...")
            with open(outpath) as f:
                return json.load(f)
        # get for the quote_home_xrate value of the stats
        currencies = list(set([f"{r.contract.currency}GBP" for r in rows]))

        # also get xrate for the fee if the fee is in a different currency to the contract
        for row in rows:
            for fee_type, fee_value, fee_currency, is_percent in row.fees:
                if fee_currency != row.contract.currency:
                    currencies.append(f"{fee_currency}{row.contract.currency}")

        print("-----> Getting FX Rates from API...")
        url = f"https://marketdata.tradermade.com/api/v1/live?currency={','.join(currencies)}&api_key={TRADERMADE_KEY}"
        resp = requests.get(url)
        data = resp.json()
        with open(outpath, "w") as f:
            json.dump(data, f, indent=4)

        return data

    async def _close_price_ib_api(self, contract):
        """
        get contract last closing prices using IB API
        used as a fallback if the exchange info page does not contain a closing price for the instrument
        some exchange info pages contain n/a for the closing price, eg NSE|NIFTY50
        """
        now = pd.Timestamp.utcnow()
        bars = await self.historic.request_bars2(
            contract=contract,
            bar_size=BarSize._1_DAY,
            what_to_show=WhatToShow.MIDPOINT,
            start_time=now - pd.Timedelta(days=10),
            end_time=now,
        )
        assert len(bars) > 0
        return bars[-1].close

    def _close_price_product_page(self, exchange, symbol, url):
        """
        downloads closing price from exchange page
        saves the html file to disk to reduce CAPTCHA amount on reruns (proxy not yet implemented)

        """
        outpath = self.parent_out / "exchange_info" / f"{exchange}-{symbol}.html"
        outpath.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(outpath) as f:
                html = f.read()
        except FileNotFoundError:
            print("-----> Getting Contract Price from Web...")
            response = requests.get(url)
            # Raise an exception for non-200 status codes
            response.raise_for_status()
            with open(outpath, "wb") as f:
                f.write(response.content)
            html = response.content
            print(f"Downloaded and saved HTML content from {url} to {outpath}")
        else:
            print("-----> Getting Contract Price from File...")
        soup = BeautifulSoup(html, "html.parser")
        close_price = soup.find(string="Closing Price").find_next("td").text
        print("product page close price: ", close_price)
        # try:
        if close_price == "n/a":
            return None
        close_price = float(close_price)
        # except Exception as e:
        # print(e)
        # return None
        return close_price

    def _contract_prices(self, rows, details):
        # filter rows to only the rows that contain percent fees
        # rows_with_percent_fees = [r for r in rows if any(fee for fee in row.fees if fee[3])]

        # load the data that already exists
        outpath = Path(self.parent_out / "fees_contract_prices.py")
        try:
            with open(outpath) as f:
                prices = json.load(f)
        except FileNotFoundError:
            prices = {}

        for r, detail in zip(rows, details):
            key = f"{r.contract.exchange}-{r.contract.symbol}"

            # if row does not contain percent fees, then continue
            contains_percent_fees = [f for f in r.fees if f[3]]
            # print("contains_percent_fees", contains_percent_fees)
            if not contains_percent_fees:
                continue

            # if the data already exists (for the day)
            if key in prices:
                continue

            outpath.parent.mkdir(parents=True, exist_ok=True)
            close_price = self._close_price_product_page(r.contract.exchange, r.contract.symbol, r.ib_url)
            if close_price is None:
                close_price = asyncio.get_event_loop().run_until_complete(self._close_price_ib_api(detail.contract))

            prices[key] = close_price
        print(prices)
        with open(outpath, "w") as f:
            json.dump(prices, f, indent=4)
        return prices

    def _calculate_fees_for_row(prices, fx_rates, fees):
        """
        calculates fees for the row and returns the modified fees
            - returns fees in the same list[tuple] structure as the input
        """
        fees = []
        for fee_type, fee_value, fee_currency, is_percent in row.fees:
            # get the fee value if the fee is the percent of the contract price
            if is_percent:
                key = f"{r.contract.exchange}-{r.contract.symbol}"
                fee_value = prices[key] * fee_value

            if r.contract.currency != fee_currency:
                quote_contract_xrate = (fx_rates[f"{fee_currency}{row.contract.currency}"],)
                fee_value = quote_contract_xrate * fee_value
                fee_type = row.contract.currency

        fees.append((fee_type, fee_value, fee_currency, is_percent))
        return fees

    def _fees(self, prices, fx_rates, rows):
        """
            calculates fees for an instrument
            - starts with the fee amounts listed on the IB website (copied into universe.csv)
            - if the fee is as a percentage of the contract currency, calculate the fee from the contract value
            - if the fee is in a different denomination than the contract currency, get the fx / exchange rate to convert the fee to the contract currency

        fee types:
            - NA Exchanges fixed_fee, contract currency, no percent
            - NA Exchange ex_fee, always USD, no percent
            - NA Exchange reg_fee, always USD, no percent
            - NA exchange clearing fee, contract currency, percent (SMFE only)
            - non NA exchanges fixed_fee, contract currency, no percent
            - non NA exchanges fixed_fee, contract currency, percent (SGX Single Stock Futures, KSE Only)
            - non NA exchange clearing_fees, contract currency, percent (SGX Single Stock Futures)

        """
        # save the unprocessed fees
        outpath = Path(self.parent_out / "fees_data.py")
        with open(outpath, "w") as f:
            json.dump([r.fees for r in rows], f, indent=4)

        fees_xrate = [self._calculate_fees_for_row(prices, fx_rates, r.fees) for r in rows]
        outpath = Path(self.parent_out / "fees_calculated.py")
        with open(outpath, "w") as f:
            json.dump(fees_xrate, f, indent=4)

        return fees_xrate

    def test_last_close(self):
        asyncio.get_event_loop().run_until_complete(self.client.connect())
        rows = [r for r in PyfuturesTestProviderStubs().universe_rows()]
        details = [asyncio.get_event_loop().run_until_complete(self._details(contract=r.contract)) for r in rows]
        for d in details:
            try:
                asyncio.get_event_loop().run_until_complete(self._close_price_ib_api(d.contract))
            except:
                pass

    def calc(self):
        results = {}
        rows = [r for r in PyfuturesTestProviderStubs().universe_rows()]
        details = [asyncio.get_event_loop().run_until_complete(self._details(contract=r.contract)) for r in rows]

        fx_rates = self._fx_rates(rows)
        prices = self._contract_prices(rows, details)
        fees = self._fees(prices, fx_rates, rows)

        for row in universe_rows:
            pass
            #
            # excel columns
            # multiplier = contract_multiplier / price_magnifier
            # multiplier_home = multiplier * quote_home_xrate
            # last_price = last_price
            # contract_notional_value = multiplier * last_price
            # contract_notional_value_gbp = multiplier_home * last_price
            # average_spread = np.mean(spread_samples)
            # average_daily_volume = np.mean(adjusted_prices_series.volume[-20:])
            # std_price_returns = std_price_returns(adjusted_prices_series, true)
            # std_price_returns_annual = std_price_returns * 16
            # std_percent_price_returns = (std_price_returns / last_price) * 100
            # std_percent_price_returns_annual = (std_price_returns_annual / last_price) * 100
            # annual_contract_risk = std_price_returns_annual * multiplier_home
            # volume_risk = (average_daily_volume * annual_contract_risk) / 1000000
            # cost_sr_units = (((average_spread * multiplier) + (commission * 2)) / contract_notional_value) / (std_percent_price_returns_annual / 100)

    # async def setup_client(self):
    #     if hasattr(self, "hclient"):
    #         return
    #     self.hclient = HistoricInteractiveBrokersClient(
    #         host="127.0.0.1", port=4002, log_level="DEBUG"
    #     )
    #     await self.hclient._client.wait_until_ready()
    #
