import asyncio
import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz
from nautilus_trader.adapters.interactive_brokers.common import (
    IBContract,
)
from nautilus_trader.adapters.interactive_brokers.historic.client import (
    HistoricInteractiveBrokersClient,
)
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor

from pyfutures.tests.adapters.interactive_brokers.test_kit import (
    IBTestProviderStubs as PyfuturesTestProviderStubs,
)

logger = Logger(name="data_stats")


def _log(msg: str):
    logger.info(msg, color=LogColor.BLUE)


# fx_rate -> prev day close mid price


class DataStats:
    def __init__(self, parent_out: Path = None):
        if parent_out is None:
            # "%Y-%m-%d_%H-%M-%S"
            self.parent_out = Path.home() / "Desktop" / "ib_data_download"

        yesterday = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        self.parent_out = self.parent_out / yesterday
        self.parent_out.mkdir(parents=True, exist_ok=True)

    async def setup_client(self):
        self.hclient = HistoricInteractiveBrokersClient(
            host="127.0.0.1", port=4002, log_level="DEBUG"
        )
        await self.hclient._client.wait_until_ready()

    async def data(self):
        await self.setup_client()
        details = await self._contract_details()
        quotes_currencies = [d.contract.currency for d in details]
        fx_rates = await self._fx_rates(quotes_currencies)

    async def _contract_details(self):
        """
        get contract_details from a pickle file in local folder
        or get from IB api if the pickle file does not exist
        """
        universe_details = []
        print("-----> Getting Contract Details...")
        for row in PyfuturesTestProviderStubs().universe_rows():
            outpath = Path(
                self.parent_out
                / "details"
                / f"{row.contract.exchange}-{row.contract.symbol}.pickle"
            )
            if outpath.exists():
                print("-----> Getting Contract Details from file...")
                with open(outpath, "rb") as f:
                    details = pickle.load(f)
                    details.append(details[0])
            else:
                print("-----> Getting Contract Details from API...")
                details = await self.hclient._client.get_contract_details(row.contract)
                if details is None:
                    print(row.contract)
                    raise ValueError("No contract found for instrument...")
                with open(outpath, "wb") as f:
                    pickle.dump(details, f)
                universe_details.append(details[0])
        # for d in details:
        # print(d.contract.currency)
        return universe_details

    async def _fx_rates():
        pass

    async def _fx_rates_ib(self, currencies):
        # load fx rates from file
        # currencies = set(currencies)
        print(currencies)
        exit()
        fx_rates_json_file = Path(self.parent_out / "fx_rates.json")
        try:
            with open(fx_rates_json_file, "r") as f:
                fx_rates = json.loads(f)
        except:
            fx_rates = {}

        # get any quote currency fx rates passed in and add+write them to the dict
        for currency in currencies:
            fx_rate_symbol = f"{currency}.GBP"
            if fx_rate_symbol in fx_rates:
                print("-----> fx rate already exists - Skipping...")
                continue
            else:
                fx_rates.setdefault(fx_rate_symbol, "")
                fx_rates[fx_rate_symbol] = await self.get_fx_rate_ib(currency)
                print("-----> Writing FX Rates for instruments...")
                with open(fx_rates_json_file, "w") as f:
                    json.dump(fx_rates, f, indent=4)
        return fx_rates

    async def get_fx_rate_ib(self, currency):
        """
        INRGBP or GBPINR -> this does not exist on IB Forex
        """
        print(f"-----> Getting fx rate {currency}.GBP...")
        print("here is a different change")
        if currency == "GBP":
            return 1.0
        elif currency == "INR":
            return 0.0096
        contract = IBContract(
            secType="CASH", exchange="IDEALPRO", currency=currency, symbol="GBP"
        )
        try:
            bars = await self._last_close_mid_bars(contract)
        except:
            print(f"-----> Getting reverse fx rate GBP.{currency}...")
            contract = IBContract(
                secType="CASH", exchange="IDEALPRO", currency="GBP", symbol=currency
            )
            bars = await self._last_close_mid_bars(contract)
            assert len(bars) > 0, "get_fx_rate: Cannot get contract or reverse contract"
            return 1 / float(bars[-1].close)
        else:
            assert len(bars) > 0
            return float(bars[-1].close)

        # details = await self.hclient._client.get_contract_details(contract=contract)
        # print("LENGTH OF DETAILS")
        # print(len(details))
        # close_price = await self._last_close_mid_price(contract)
        # if details is None:
        # details = await self.hclient._client.get_contract_details(contract=contract)
        # if details is None:
        # return 1 / close_price

        # print(f"{currency}GBP not found... Using GBP.{currency}")
        # return close_price

    async def _last_close_mid_bars(self, contract):
        bars = await self.hclient.request_bars(
            bar_specifications=["1-DAY-MID"],
            end_date_time=datetime.now(),
            start_date_time=datetime.now() - timedelta(days=10),
            contracts=[contract],
            tz_name=pytz.utc,
        )
        # print(bars)
        # print(type(bars[-1].close))
        # print(bars[-1].close)
        return bars

    def calculate_fees(self, row):
        total_fee = 0
        fees = []
        for key in ["fee_fixed", "fee_exchange", "fee_regulatory", "fee_clearing"]:
            fee_value = row[key]
            is_percent = "%" in fee_value
            fee_currency = row[key + "_currency"]

            fee_value = fee_value.replace("%", "")

            # convert percent fee to static fee
            if is_percent:
                bars = row._last_close_mid_bars(row.contract)
                assert len(bars) > 0
                fee_value = self.contract_price * (float(fee_value) / 100)

            if fee_value != 0 and row.contract.currency != fee_currency:
                pass
            elif fee_value != 0:
                pass

        # sum fees here

    def write(self, dst):
        outpath.parent.mkdir(parents=True, exist_ok=True)


data_stats = DataStats()
# print(asyncio.run(data_stats.get_fx_rate("EUR")))
asyncio.run(data_stats.data())
