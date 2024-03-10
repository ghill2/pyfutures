import logging
from pathlib import Path

import pandas as pd

from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import WhatToShow
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.stats.cache import async_cache_json_daily
from pyfutures.stats.fx_rates import FxRates
from pyfutures.tests.test_kit import UniverseRow


class FeeCalculator:
    """
    Fees are returned from the csv file in format:
    [(fee_type, fee_value, fee_currency, is_percent), (...)]
    """

    def __init__(self, client: InteractiveBrokersClient):
        # self.client = InteractiveBrokersClient(loop=asyncio.get_event_loop())
        self.client = client
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.setLevel(logging.DEBUG)
        # store the logs in the cache
        now = pd.Timestamp.utcnow()
        path = Path.home() / "Desktop" / "pyfutures_cache" / "fees" / now.strftime("%Y-%m-%d")
        path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path / f"{now}.log")
        file_handler.setLevel(logging.DEBUG)
        self._log.addHandler(file_handler)
        # logger.addHandler(stdout_handler)

    @async_cache_json_daily(dir="fees", filename="universe_fees")
    async def calculate_rows(self, rows):
        """
        calculates fees across multiple rows / instruments
        """
        if isinstance(rows, UniverseRow):
            rows = [rows]

        # gather tradermade API exchange rates
        currencies = set()
        # currencies = [f"{fee['currency']}{row.quote_currency.code}" for row in rows for fee in row.fees if fee["currency"] != row.quote_currency.code]
        for row in rows:
            for fee in row.fees:
                fee_currency = fee["currency"]
                quote_currency = row.quote_currency.code
                if quote_currency != fee_currency:
                    # then the fee needs conversion
                    quote_xrate_pair = f"{fee_currency}{quote_currency}"
                    currencies.add(quote_xrate_pair)  # quote_contract_xrate
        self.fx_rates = FxRates().get(currencies)

        # creates and connects a client
        if not self.client.connection.is_connected:
            await self.client.connect()
            await self.client.request_market_data_type(4)

        fees = {}
        for r in rows:
            instrument_id = f"{r.trading_class}={r.symbol}.{r.exchange}"
            fees[instrument_id] = await self._calculate_row(r)

        return fees

    async def _calculate_row(self, row):
        """
            requires calculate_rows to get the data for the calculations

        for every fee:
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
        assert self.fx_rates, "You must call self.calculate_rows() before this method"
        self._log.debug(f" ========== {row.exchange}|{row.trading_class}|{row.quote_currency.code} ==========")
        fees = row.fees.copy()
        for i, fee in enumerate(fees):
            fee_name = fee["name"]
            fee_value = fee["value"]
            fee_currency = fee["currency"]
            fee_is_percent = fee["is_percent"]
            self._log.debug(f"--> {i + 1}/{len(fees)} {fee_name} {fee_value}{'%' if fee_is_percent else ''} {fee_currency}")
            if fee_is_percent:
                last_bar = await self.client.request_last_bar(contract=row.contract_cont, bar_size=BarSize._1_DAY, what_to_show=WhatToShow.TRADES)
                last_close_price = last_bar.close
                calc_value = fee_value * last_close_price
                self._log.debug(
                    f"---> to_fixed: {fee_value}% {fee_currency} = {fee_value} * {last_close_price} {fee_currency} = {calc_value} {fee_currency}"
                )
                fee_value = calc_value
                fee_is_percent = False

            quote_currency = row.quote_currency.code
            if quote_currency != fee_currency:
                quote_xrate_pair = f"{fee_currency}{quote_currency}"
                fx_rate = self.fx_rates[quote_xrate_pair]
                calc_value = fee_value * fx_rate
                self._log.debug(
                    f"---> exchange_rate: {fee_value} {fee_currency} = {fee_value} * {fx_rate} ({quote_xrate_pair}) = {calc_value} {quote_currency}"
                )

                fee_value = calc_value
                fee_currency = quote_currency

            fee["value"] = fee_value
            fee["currency"] = fee_currency
            fee["is_percent"] = fee_is_percent

        # all currencies should match before summing
        all(f["currency"] == fees[0]["currency"] for f in fees[1:])

        # display only
        # sum_str = "sum"
        # for i, fee in enumerate(fees):
        #     sum_str = sum_str + f"{fee['value']} {fee['currency']}"
        #     if i < len(fees):
        #         sum_str = f"{sum_str} +"
        #     else:
        #         sum_str = f"{sum_str} ="
        fee_values_display = [f"{fee['value']} {fee['currency']}" for fee in fees]
        fee_values_str = "-> sum: " + " + ".join(fee_values_display) + " ="

        total = sum(f["value"] for f in fees)
        self._log.debug(f"{fee_values_str} {total} {row.quote_currency.code}")

        return dict(original_fees=row.fees, fees=fees, sum=total)


# def _last_close_price(self, row):
#       """
#       fetches contract prices for the instrument
#       """
#       html = self.product_info.download(url)
#       print(html)
#       print(url)
#       soup = BeautifulSoup(html, "html.parser")
#       close_price = soup.find(string="Closing Price").find_next("td").text
#       close_price = float(close_price)
#
#
