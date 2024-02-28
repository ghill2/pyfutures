import pytest
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from pyfutures.adapters.interactive_brokers.cache import CachedFunc
from ibapi.contract import Contract as IBContract
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
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
        
    @pytest.mark.asyncio()
    async def test_call_writes_client_exception(self):
        cached_func = CachedFunc(func=self.request_bars_with_client_exception)
        await cached_func(**self._cached_func_kwargs)
        assert cached_func.json_path(**self._cached_func_kwargs).exists()
    
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
    