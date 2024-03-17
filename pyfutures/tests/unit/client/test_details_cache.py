import tempfile
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails

from pyfutures.client.cache import DetailsCache


def create_detail():
    contract = IBContract()
    contract.secType = "CONTFUT"
    contract.tradingClass = "DA"
    contract.symbol = "DC"
    contract.exchange = "CME"
    detail = IBContractDetails()
    detail.contract = contract
    return detail


class TestDetailsCache:
    def setup_method(self):
        self.detail = create_detail()
        self.cache = DetailsCache(path=tempfile.mkdtemp())

    def test_get_returns_none_if_not_cached(self):
        assert self.cache.get(key="test") is None

    def test_details_roundtrip(self):
        self.cache.set(key="test", value=[self.detail])
        details = self.cache.get(key="test")
        assert details[0].contract.secType == self.detail.contract.secType
        assert details[0].contract.tradingClass == self.detail.contract.tradingClass
        assert details[0].contract.symbol == self.detail.contract.symbol
        assert details[0].contract.exchange == self.detail.contract.exchange


class TestDetailsCachedFunc:
    def setup_method(self):
        self.detail = create_detail()
        self.cached_func_kwargs = dict(contract=self.detail.contract)

        self.cached_func = CachedFunc(func=self.request_contract_details, cache=DetailsCache(path=tempfile.mkdtemp()))

    def test_build_key(self):
        # NOTE: if this failed test it means the cache has been invalidated
        expected = "DA-DC-CME-CONTFUT"
        key = self.cached_func._cache.build_key(**self.cached_func_kwargs)
        assert key == expected

    @pytest.mark.asyncio()
    async def test_call_returned_expected_cached(self):
        self.cached_func.get = Mock(return_value=[self.detail])
        details = await self.cached_func(**self.cached_func_kwargs)

        assert details[0].contract.secType == self.detail.contract.secType
        assert details[0].contract.tradingClass == self.detail.contract.tradingClass
        assert details[0].contract.symbol == self.detail.contract.symbol
        assert details[0].contract.exchange == self.detail.contract.exchange

    @pytest.mark.asyncio()
    async def test_call_returned_expected_uncached(self):
        details = await self.cached_func(**self.cached_func_kwargs)

        assert details[0].contract.secType == self.detail.contract.secType
        assert details[0].contract.tradingClass == self.detail.contract.tradingClass
        assert details[0].contract.symbol == self.detail.contract.symbol
        assert details[0].contract.exchange == self.detail.contract.exchange

    @pytest.mark.asyncio()
    async def test_call_writes_uncached_bar_data(self):
        await self.cached_func(**self.cached_func_kwargs)
        key = self.cached_func._cache.build_key(**self.cached_func_kwargs)
        assert self.cached_func._cache.get(key) is not None

    async def request_contract_details(
        self,
        contract: IBContract,
    ) -> IBContractDetails:
        return [self.detail]
