import pytest
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from pyfutures.adapter.cache import CachedFunc
from ibapi.contract import Contract as IBContract
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.client.objects import ClientException
from pyfutures.adapter.enums import WhatToShow
import pandas as pd
from ibapi.common import BarData

class TestCachedFunc:
    
    def setup_method(self):
        bar = BarData()
        bar.timestamp = pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        bar.date = 1700069390
        bar.open = 0.
        bar.high = 0.
        bar.low = 0.
        bar.close = 0.
        bar.volume = Decimal("1")
        bar.wap = Decimal("1")
        bar.barCount = 0
        self.bar = bar
        
        
        contract = IBContract()
        contract.secType = "CONTFUT"
        contract.tradingClass = "DA"
        contract.symbol = "DC"
        contract.exchange = "CME"
        self._cached_func_kwargs = dict(
            contract=contract,
            bar_size=BarSize._1_DAY,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-01 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-01 08:00:00", tz="UTC"),
        )
        
    @pytest.mark.asyncio()
    async def test_bar_data_round_trip(self):
        cached_func = CachedFunc(func=self.request_bars)
        cached_func._set(key="test", value=[self.bar])
        bars = cached_func._get(key="test")
        assert bars[0].timestamp == self.bar.timestamp
        assert bars[0].date == self.bar.date
        assert bars[0].open == self.bar.open
        assert bars[0].high == self.bar.high
        assert bars[0].low == self.bar.low
        assert bars[0].close == self.bar.close
        assert bars[0].volume == self.bar.volume
        assert bars[0].wap == self.bar.wap
        assert bars[0].barCount == self.bar.barCount
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_call_writes_expected_key(self):
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_is_cached(self):
        pass
        
    @pytest.mark.asyncio()
    async def test_client_exception_round_trip(self):
        cached_func = CachedFunc(func=self.request_bars)
        exception = ClientException(code=123, message="test")
        cached_func._set(key="test", value=exception)
        cached = cached_func._get(key="test")
        assert cached == exception
        
    @pytest.mark.asyncio()
    async def test_client_exception_round_trip(self):
        cached_func = CachedFunc(func=self.request_bars)
        exception = asyncio.TimeoutError()
        cached_func._set(key="test", value=exception)
        cached = cached_func._get(key="test")
        assert isinstance(cached, asyncio.TimeoutError)
    
    @pytest.mark.asyncio()
    async def test_call_writes_timeout_error(self):
        cached_func = CachedFunc(func=self.request_bars_with_timeout_error)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_purge_errors(self):
        cached_func = CachedFunc(func=self.request_bars_with_timeout_error)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        cached_func.purge_errors(asyncio.TimeoutError)
        assert not cached_func.get_cached_path(**self._cached_func_kwargs).exists()
    
    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[BarData]:
        return [self.bar]
    
    async def request_bars_with_client_exception(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[BarData]:
        return ClientException(code=123, message="test")
    
    async def request_bars_with_timeout_error(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[BarData]:
        return asyncio.TimeoutError()
    
    
# def test_purge_cache():
#     request_bars = RequestBarsCache(
#         client=self._client,
#         name="request_bars",
#         timeout_seconds=60 * 10,
#     )
    