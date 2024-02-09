import asyncio

import pandas as pd
from ibapi.common import BarData
from ibapi.contract import Contract as IBContract

from nautilus_trader.common.component import Logger
from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime


class InteractiveBrokersHistoric:
    def __init__(self, client: InteractiveBrokersClient, logger: Logger):
        self._client = client
        self._log = Logger(type(self).__name__, logger)

    async def download(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
    ):
        """
        Downloads all the data for a contract.
        """
        # assert bar_size in (BarSize._1_DAY, BarSize._1_HOUR, BarSize._1_MINUTE)

        # get start timestamp
        head_timestamp = await self._client.request_head_timestamp(
            contract=contract,
            what_to_show=what_to_show,
        )

        print(head_timestamp)
        if head_timestamp is None:
            print(
                f"No head timestamp for {contract.symbol} {contract.exchange} {contract.lastTradeDateOrContractMonth} {contract.conId}",
            )
            return None
        else:
            print(
                f"Head timestamp found: {head_timestamp}  {contract.symbol} {contract.exchange} {contract.lastTradeDateOrContractMonth} {contract.conId}",
            )

        # set an appopriate interval range depending on the desired data frequency
        if bar_size.frequency == Frequency.DAY:
            duration = Duration(step=365, freq=Frequency.DAY)
            freq = "365 D"
        elif bar_size.frequency == Frequency.HOUR or bar_size.frequency == Frequency.MINUTE:
            duration = Duration(step=1, freq=Frequency.DAY)
            freq = "1 D"

        total_bars = []
        end_time = pd.Timestamp.utcnow().ceil(freq)

        while end_time >= head_timestamp:
            print(contract.symbol, contract.exchange, f"head_timestamp={head_timestamp}")

            self._log.debug(
                f"--> ({contract.symbol}{contract.exchange}) "
                f"Downloading bars at interval: {end_time}",
            )

            try:
                bars: list[BarData] = await self._client.request_bars(
                    contract=contract,
                    bar_size=str(bar_size),
                    what_to_show=what_to_show,
                    duration=str(duration),
                    end_time=end_time,
                    timeout_seconds=100,
                )
            except ClientException as e:
                if e.code != 162:
                    raise e

                # Historical Market Data Service error message:HMDS query returned no data
                await asyncio.sleep(2)

                end_time -= pd.Timedelta(freq)
                print(f"No data for end_time {end_time}")

                continue

            print(f"Downloaded {len(bars)} bars...")

            total_bars.extend(bars)

            end_time = parse_datetime(bars[0].date)

            await asyncio.sleep(3)

        return self._parse_dataframe(list(reversed(total_bars)))

    @staticmethod
    def _parse_dataframe(bars: list[BarData]) -> pd.DataFrame:
        
        return pd.DataFrame(
            {
                "date": [parse_datetime(bar.date) for bar in bars],
                "open": [bar.open for bar in bars],
                "high": [bar.high for bar in bars],
                "low": [bar.low for bar in bars],
                "close": [bar.close for bar in bars],
                "volume": [float(bar.volume) for bar in bars],
                "wap": [float(bar.wap) for bar in bars],
                "barCount": [bar.barCount for bar in bars],
            },
        )
