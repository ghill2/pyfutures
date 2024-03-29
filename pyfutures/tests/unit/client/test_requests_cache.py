import asyncio
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.contract import Contract as IBContract

from pyfutures.client.cache import RequestsCache
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.client.objects import ClientException


class TestRequestsCache:
    def setup_method(self):
        bar = BarData()
        bar.timestamp = pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        bar.date = 1700069390
        bar.open = 0.0
        bar.high = 0.0
        bar.low = 0.0
        bar.close = 0.0
        bar.volume = Decimal("1")
        bar.wap = Decimal("1")
        bar.barCount = 0
        self.bar = bar

        self.cache = RequestsCache(path=tempfile.mkdtemp())

    def test_get_returns_none_if_not_cached(self):
        assert self.cache.get(key="test") is None

    def test_bar_data_round_trip(self):
        self.cache.set(key="test", value=[self.bar])
        bars = self.cache.get(key="test")
        assert bars[0].timestamp == self.bar.timestamp
        assert bars[0].date == self.bar.date
        assert bars[0].open == self.bar.open
        assert bars[0].high == self.bar.high
        assert bars[0].low == self.bar.low
        assert bars[0].close == self.bar.close
        assert bars[0].volume == self.bar.volume
        assert bars[0].wap == self.bar.wap
        assert bars[0].barCount == self.bar.barCount

    def test_client_exception_round_trip(self):
        ex = ClientException(code=123, message="test")
        self.cache.set(key="test", value=ex)
        cached = self.cache.get(key="test")
        assert cached == ex

    def test_client_exception_round_trip(self):
        ex = asyncio.TimeoutError()
        self.cache.set(key="test", value=ex)
        cached = self.cache.get(key="test")
        assert isinstance(cached, asyncio.TimeoutError)

    def test_purge_timeout_error(self):
        self.cache.set(key="test_bar", value=[self.bar])
        self.cache.set(key="test_exception", value=asyncio.TimeoutError())
        assert (self.cache.path / "test_exception.pkl").exists()
        self.cache.purge_errors()
        assert not (self.cache.path / "test_exception.pkl").exists()
        assert (self.cache.path / "test_bar.parquet").exists()

    def test_purge_client_exception(self):
        ex = ClientException(code=123, message="test")
        self.cache.set(key="test_bar", value=[self.bar])
        self.cache.set(key="test_exception", value=ex)
        assert (self.cache.path / "test_exception.pkl").exists()
        self.cache.purge_errors()
        assert not (self.cache.path / "test_exception.pkl").exists()
        assert (self.cache.path / "test_bar.parquet").exists()


class TestRequestsCachedFunc:
    def setup_method(self):
        bar = BarData()
        bar.timestamp = pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        bar.date = 1700069390
        bar.open = 0.0
        bar.high = 0.0
        bar.low = 0.0
        bar.close = 0.0
        bar.volume = Decimal("1")
        bar.wap = Decimal("1")
        bar.barCount = 0
        self.bar = bar

        contract = IBContract()
        contract.secType = "CONTFUT"
        contract.tradingClass = "DA"
        contract.symbol = "DC"
        contract.exchange = "CME"
        self.cached_func_kwargs = dict(
            contract=contract,
            bar_size=BarSize._1_DAY,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-01 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-01 08:00:00", tz="UTC"),
        )

        self.cached_func = CachedFunc(
            func=self.request_bars, cache=RequestsCache(path=Path(tempfile.mkdtemp()))
        )

    def test_build_key(self):
        # NOTE: if this failed test it means the cache has been invalidated
        expected = (
            "DA-CME-CONTFUT=1-day=BID_ASK=2023-01-01-00-00-00=2023-01-01-08-00-00"
        )
        key = self.cached_func._cache.build_key(**self.cached_func_kwargs)
        assert key == expected

    @pytest.mark.asyncio()
    async def test_call_returns_expected_cached_bar_data(self):
        self.cached_func.get = Mock(return_value=[self.bar])

        bars = await self.cached_func(**self.cached_func_kwargs)

        assert bars[0].timestamp == self.bar.timestamp
        assert bars[0].date == self.bar.date
        assert bars[0].open == self.bar.open
        assert bars[0].high == self.bar.high
        assert bars[0].low == self.bar.low
        assert bars[0].close == self.bar.close
        assert bars[0].volume == self.bar.volume
        assert bars[0].wap == self.bar.wap
        assert bars[0].barCount == self.bar.barCount

    @pytest.mark.asyncio()
    async def test_call_returns_expected_uncached_bar_data(self):
        bars = await self.cached_func(**self.cached_func_kwargs)

        assert bars[0].timestamp == self.bar.timestamp
        assert bars[0].date == self.bar.date
        assert bars[0].open == self.bar.open
        assert bars[0].high == self.bar.high
        assert bars[0].low == self.bar.low
        assert bars[0].close == self.bar.close
        assert bars[0].volume == self.bar.volume
        assert bars[0].wap == self.bar.wap
        assert bars[0].barCount == self.bar.barCount

    @pytest.mark.asyncio()
    async def test_call_raises_cached_exception(self):
        exception = ClientException(code=123, message="test")
        self.cached_func._cache.get = Mock(return_value=exception)

        with pytest.raises(ClientException):
            await self.cached_func(**self.cached_func_kwargs)

    @pytest.mark.asyncio()
    async def test_call_writes_uncached_bar_data(self):
        await self.cached_func(**self.cached_func_kwargs)
        key = self.cached_func._cache.build_key(**self.cached_func_kwargs)
        assert self.cached_func._cache.get(key) is not None

    @pytest.mark.asyncio()
    async def test_is_cached(self):
        await self.cached_func(**self.cached_func_kwargs)
        assert self.cached_func.is_cached(**self.cached_func_kwargs)

    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[BarData]:
        return [self.bar]
