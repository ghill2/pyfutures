import asyncio

import pandas as pd
from ibapi.common import BarData
from ibapi.contract import Contract as IBContract
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.uuid import UUID4

from nautilus_trader.common.component import Logger
from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime
from pyfutures.adapters.interactive_brokers.parsing import historical_tick_bid_ask_to_dict
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick

class InteractiveBrokersHistoric:
    def __init__(self, client: InteractiveBrokersClient, delay: float = 0):
        self._client = client
        self._log = Logger(type(self).__name__)
        self._delay = delay

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
    
    async def request_quote_ticks(
        self,
        contract: IBContract,
        start_time: pd.Timestamp = None,
        end_time: pd.Timestamp = None,
        as_dataframe: bool = False,
    ):
        
        """
        if end_time is passed only:
            start_time=None, end_time=end_time
            first_tick = x
            break on first tick timestamp
        
        if start_time is passed only:
            start_time=start_time, end_time=None
            last_tick = x
            break on last tick timestamp
        
        if start_time is passed and end_time is passed:
            start_time=None, end_time=end_time
            first_tick = x
            break on first tick timestamp
        """
        
        assert start_time is not None and end_time is not None
        
        results = []
        while True:
            
            # split time range into interval, floor start and ceil end
            # for each interval, reduce end time until empty list
            
            quotes: list[IBQuoteTick] = await self._client.request_quote_ticks(
                contract=contract,
                end_time=end_time,
            )
            
            if len(quotes) == 0 and len(results) == 0:
                break  # no quotes within range
            
            results.extend(quotes)
            
            end_time = parse_datetime(results[0].time)
            if end_time <= start_time:
                break
            
            if self._delay > 0:
                asyncio.sleep(self._delay)
            
        results = [
            x for x in results if parse_datetime(x.time) >= start_time
        ]
        
        if as_dataframe:
            return pd.DataFrame(
                [
                    historical_tick_bid_ask_to_dict(obj)
                    for obj in results
                ]
            )
        return results
        
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


        
            # head_timestamp = await self._client.request_head_timestamp(
            #     contract=contract,
            #     what_to_show=WhatToShow.BID_ASK,
            # )
            # assert start_time >= head_timestamp
            
            # quotes: list[IBQuoteTick] = await self._client.request_quote_ticks(
            #     name=str(UUID4()),
            #     contract=contract,
            #     start_time=head_timestamp + pd.Timedelta(days=200),
            #     # end_time=head_timestamp + pd.Timedelta(days=14),
            #     count=1,
            # )
            # start_time = quotes[0].time
        # assert start_time < end_time