import pytest
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

class TestCachedFunc:
    
    def setup_method(self):
        bar = BarData()
        bar.timestamp = pd.Timestamp("2023-01-01", tz="UTC")
        self.open = 0.
        self.high = 0.
        self.low = 0.
        self.close = 0.
        self.volume = Decimal("1")
        self.wap = Decimal("1")
        self.barCount = 0
        self._bar = bar
        
        
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
    async def test_call_writes_bar_data(self):
        cached_func = CachedFunc(func=self.request_bars)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_call_writes_client_exception(self):
        cached_func = CachedFunc(func=self.request_bars_with_client_exception)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_call_writes_timeout_error(self):
        cached_func = CachedFunc(func=self.request_bars_with_timeout_error)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        
    @pytest.mark.asyncio()
    async def test_call_reads_bar_data(self):
        cached_func = CachedFunc(func=self.request_bars)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        bars = await cached_func(**self._cached_func_kwargs)
        assert str(bars[0]) == str(self._bar)  # no equality implemented on BarData
        assert bars[0].timestamp == self._bar.timestamp
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_call_reads_client_exception(self):
        cached_func = CachedFunc(func=self.request_bars_with_client_exception)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        exception = await cached_func(**self._cached_func_kwargs)
        assert isinstance(exception, ClientException)
        assert exception.code == 123
        assert exception.message == "Error 123: test"
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_call_reads_client_exception(self):
        cached_func = CachedFunc(func=self.request_bars_with_timeout_error)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.get_cached_path(**self._cached_func_kwargs).exists()
        exception = await cached_func(**self._cached_func_kwargs)
        assert isinstance(exception, asyncio.TimeoutError)
    
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
        return [self._bar]
    
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
    