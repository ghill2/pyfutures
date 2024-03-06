import pytest
import random
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from pyfutures.adapter.cache import CachedFunc
from ibapi.contract import Contract as IBContract
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.client.objects import ClientException
from pyfutures.adapter.enums import WhatToShow
import pandas as pd
from ibapi.common import BarData
from unittest.mock import Mock

class TestHistoric:
    
    def setup_method(self):
        self.contract = IBContract()
        self.contract.secType == "CONTFUT"
        self.contract.tradingClass == "DC"
        self.contract.symbol == "DA"
        self.contract.exchange == "CME"
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_bars_limit(self, historic):
        pass
        
    @pytest.mark.asyncio()
    async def test_request_bars_returns_expected_dataframe(self, historic):
        send_mock = AsyncMock(side_effect=self.request_bars)
        historic._client.request_bars = send_mock
        historic.request_bars_cache = send_mock
        historic.request_bars_cache.is_cached = Mock(return_value=False)
        
        df = await historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-03 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            cache=True,
            as_dataframe=True,
        )
        assert list(df.columns) == ["timestamp", "date", "open", "high", "low", "close", "volume", "wap", "barCount"]
        
    @pytest.mark.asyncio()
    async def test_request_bars_sends_expected(self, historic):
        
        send_mock = AsyncMock(side_effect=self.request_bars)
        historic._client.request_bars = send_mock
        historic.request_bars_cache = send_mock
        historic.request_bars_cache.is_cached = Mock(return_value=False)
        
        await historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-03 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            cache=True,
        )
        end_times = [call[1]["end_time"] for call in send_mock.call_args_list]
        assert end_times == [
            pd.Timestamp('2023-01-06 00:00:00+0000', tz='UTC'),
            pd.Timestamp('2023-01-05 00:00:00+0000', tz='UTC'),
            pd.Timestamp('2023-01-04 00:00:00+0000', tz='UTC'),
        ]
        
    @pytest.mark.asyncio()
    async def test_historic_first_request_not_use_cache(self, historic):
        # the first request will have imcomplete data because the end time is ceiled to the interval
        # do not cache the first request
        
        historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache.is_cached = Mock(return_value=True)
        
        await historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            cache=True,
        )
        
        historic._client.request_bars.assert_called_once()
        historic.request_bars_cache.assert_called_once()
    
    @pytest.mark.asyncio()
    async def test_historic_delay_if_not_cached(self, historic):
        
        historic._delay = 1
        historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache.is_cached = Mock(return_value=False)
        asyncio.sleep = AsyncMock()
               
        await historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            cache=True,
        )
        
        historic._client.request_bars.assert_called_once()
        historic.request_bars_cache.assert_called_once()
        asyncio.sleep.call_count == 2  # first and second request
    
    @pytest.mark.asyncio()
    async def test_historic_no_delay_if_cached(self, historic):
        
        historic._delay = 1
        historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache = AsyncMock(side_effect=self.request_bars)
        historic.request_bars_cache.is_cached = Mock(return_value=True)
        asyncio.sleep = AsyncMock()
        
        await historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            cache=True,
        )
        
        historic._client.request_bars.assert_called_once()
        historic.request_bars_cache.assert_called_once()
        asyncio.sleep.assert_called_once()  # first request only
    
    async def request_bars(
        self,
        **kwargs
    ) -> list[BarData]:
        end_time = kwargs["end_time"]
        start_time = end_time - pd.Timedelta(hours=24)
        time_range = ((end_time - start_time).total_seconds() / 60) - 1
        
        random_times = set()
        while len(random_times) != 30:
            random_minute = random.randint(0, time_range)
            random_time = start_time + pd.Timedelta(minutes=random_minute)
            assert random_time >= start_time and random_time < end_time
            random_times.add(random_time)
        
        bars = [BarData() for _ in range(30)]
        for i, time in enumerate(random_times):
            bars[i].timestamp = time
            bars[i].date = int(time.timestamp())
            bars[i].open = 1.1
            bars[i].high = 1.2
            bars[i].low = 1.0
            bars[i].close = 1.1
            bars[i].volume = Decimal("1")
            bars[i].wap = Decimal("1")
            bars[i].barCount = 1
        return bars