import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.contract import Contract as IBContract

from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import WhatToShow
from pyfutures.client.historic import InteractiveBrokersBarClient
from pyfutures.tests.unit.client.stubs import ClientStubs


class TestHistoric:
    def setup_method(self):
        self.historic: InteractiveBrokersBarClient = ClientStubs.historic()
        self.contract = IBContract()
        self.contract.secType == "CONTFUT"
        self.contract.tradingClass == "DC"
        self.contract.symbol == "DA"
        self.contract.exchange == "CME"

    @pytest.mark.asyncio()
    async def test_request_bars_limit(self):
        send_mock = AsyncMock(side_effect=self.request_bars)
        self.historic._client.request_bars = send_mock
        self.historic.request_bars_cache = send_mock
        self.historic.request_bars_cache.is_cached = Mock(return_value=False)

        bars = await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-03 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            limit=60,
        )
        assert len(bars) == 60

    @pytest.mark.asyncio()
    async def test_request_bars_returns_expected_dataframe(self):
        send_mock = AsyncMock(side_effect=self.request_bars)
        self.historic._client.request_bars = send_mock
        self.historic.cache = send_mock
        self.historic.cache.is_cached = Mock(return_value=False)

        df = await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-03 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
            as_dataframe=True,
        )
        assert list(df.columns) == ["timestamp", "date", "open", "high", "low", "close", "volume", "wap", "barCount"]

    @pytest.mark.asyncio()
    async def test_request_bars_sends_expected(self):
        send_mock = AsyncMock(side_effect=self.request_bars)
        self.historic._client.request_bars = send_mock
        self.historic.cache = send_mock
        self.historic.cache.is_cached = Mock(return_value=False)

        await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-03 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
        )
        end_times = [call[1]["end_time"] for call in send_mock.call_args_list]
        assert end_times == [
            pd.Timestamp("2023-01-06 00:00:00+0000", tz="UTC"),
            pd.Timestamp("2023-01-05 00:00:00+0000", tz="UTC"),
            pd.Timestamp("2023-01-04 00:00:00+0000", tz="UTC"),
        ]

    @pytest.mark.asyncio()
    async def test_historic_first_request_not_use_cache(self):
        # the first request will have imcomplete data because the end time is ceiled to the interval
        # do not cache the first request

        self.historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        self.historic.cache = AsyncMock(side_effect=self.request_bars)
        self.historic.cache.is_cached = Mock(return_value=True)

        await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
        )

        self.historic._client.request_bars.assert_called_once()
        self.historic.cache.assert_called_once()

    @pytest.mark.asyncio()
    async def test_historic_delay_if_not_cached(self):
        self.historic._delay = 1
        self.historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        self.historic.cache = AsyncMock(side_effect=self.request_bars)
        self.historic.cache.is_cached = Mock(return_value=False)
        asyncio.sleep = AsyncMock()

        await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
        )

        self.historic._client.request_bars.assert_called_once()
        self.historic.cache.assert_called_once()
        asyncio.sleep.call_count == 2  # first and second request

    @pytest.mark.skip(reason="TODO: add get response")
    @pytest.mark.asyncio()
    async def test_historic_no_delay_if_cached(self):
        self.historic._delay = 1
        self.historic._client.request_bars = AsyncMock(side_effect=self.request_bars)
        self.historic.cache = AsyncMock(side_effect=self.request_bars)
        self.historic.cache.is_cached = Mock(return_value=True)
        asyncio.sleep = AsyncMock()

        await self.historic.request_bars(
            contract=self.contract,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=pd.Timestamp("2023-01-04 00:00:00", tz="UTC"),
            end_time=pd.Timestamp("2023-01-05 02:00:00", tz="UTC"),
        )

        self.historic._client.request_bars.assert_called_once()
        self.historic.cache.assert_called_once()
        asyncio.sleep.assert_called_once()  # first request only

    async def request_bars(self, **kwargs) -> list[BarData]:
        end_time = kwargs["end_time"]
        start_time = end_time - pd.Timedelta(hours=24)

        times = [start_time + pd.Timedelta(minutes=i) for i in range(60)]

        bars = [BarData() for _ in range(60)]
        for i, time in enumerate(times):
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
