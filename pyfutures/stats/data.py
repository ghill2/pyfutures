import pickle
import asyncio
from nautilus_trader.adapters.interactive_brokers.config import (
    InteractiveBrokersGatewayConfig,
)
from nautilus_trader.adapters.interactive_brokers.historic.client import HistoricInteractiveBrokersClient
from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.adapters.interactive_brokers.common import IBContractDetails
from pathlib import Path
from pyfutures.tests.adapters.interactive_brokers.test_kit import (
    IBTestProviderStubs as PyfuturesTestProviderStubs,
)
import pandas as pd
from nautilus_trader.model.data import BarSpecification
from datetime import datetime
from datetime import timedelta
import pytz
import json

from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import Logger

# fx_rate -> prev day close mid price

class DataStats:
    def __init__(self, parent_out: Path = None):
        if parent_out is None:
            # "%Y-%m-%d_%H-%M-%S"
            self.parent_out = Path.home() / "ib_data_download"
        


    async def setup_client(self):
        self.hclient = HistoricInteractiveBrokersClient(
            host="127.0.0.1",
            port=4002,
            log_level="DEBUG"
        )



    def data(self):
        await self.setup_client()
        rows = PyfuturesTestProviderStubs().universe_rows()
        yesterday = (pd.Timestamp.utcnow() - pd.TimeDelta(days=1)).strftime("%Y-%m-%d")
        parent_out = self.parent_out / yesterday

        fx_rates = {}

        # yesterday
        for row in rows:
            exchange = row.contract.exchange
            symbol = row.contract.symbol

            print("Getting Contract Details...")
            # pickle contract details
            outpath = parent_out / "details" / f"{exchange}-{symbol}.pickle"
            outpath.mkdir(parents=True, exist_ok=True)
            details = self.hclient._client.get_contract_details(row.contract)
            front_contract = details[0]
            if outpath.exists():
                with open(outpath, "wb") as f:
                    pickle.dump(details, f)

            parent_out = self.parent_out / yesterday
            setdefault(fx_rates, front_contract.currency)
            fx_rates[front_contract.currency] = self.get_fx_rates(front_contract.currency)
        print("Writing FX Rates for instruments...")
        with open(parent_out / "fx_rates.json", "w") as f:
            json.dump(fx_rates, f, indent=4)

    async def get_fx_rate(self,currency):
        print(f"Getting fx rate for {currency}...")
        close_price = await self._get_fx_rate(f"{currency}GBP")
        if close_price is None:
            print(f"{currency}GBP not found... Using GBP{currency}")
            close_price = await self._get_fx_rate(f"GBP{currency}")
            return 1 / close_price
        return close_price

    async def _get_fx_rate(self, symbol):
        contract = Forex(f"{currency}GBP")
        details = await self._client.get_contract_details(contract=contract)
        if details is None:
            return
        bars = self.hclient.request_bars(
            bar_specificiations=[BarSpecification.from_str("1-DAY-MID")],
            start_date_time="",
            end_date_time=datetime.now(pytz.utc),
            start_date_time=datetime.now(pytz.utc) - timedelta(days=-3),
            contracts=[contract],
            tz_name="UTC",
        )
        return bars[-1].close


    def write(self, dst):
        outpath.parent.mkdir(parents=True, exist_ok=True)


data_stats = DataStats()
data_stats.data()


