import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.contract import Contract
from unittest.mock import AsyncMock
import asyncio

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow


# MERGE THESE 2 TESTS BELOW
# @pytest.mark.skip(reason="flakey if market not open")
@pytest.mark.timeout(30)
@pytest.mark.asyncio()
async def test_subscribe_bars_historical(client, contract):
    await client.connect()
    contract = await client.request_front_contract(contract)

    async def wait_for_bar(bar):
        await asyncio.sleep(0)

    callback_mock = AsyncMock(side_effect=wait_for_bar)

    client.subscribe_bars(
        contract=contract,
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=callback_mock,
    )

    while callback_mock.call_count == 0:
        await asyncio.sleep(0)


@pytest.mark.asyncio()
async def test_subscribe_bars_historical_returns_expected(self):
    def send_mocked_response(*args, **kwargs):
        bar = BarData()
        bar.date = 1700069390
        self.client.historicalDataUpdate(-10, bar)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqHistoricalData = send_mock

    callback_mock = Mock()

    # Act
    self.client.subscribe_bars(
        contract=Contract(),
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=callback_mock,
    )

    # Assert
    bar = callback_mock.call_args_list[0][0][0]
    assert isinstance(bar, BarData)
    assert bar.timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")


@pytest.mark.asyncio()
async def test_unsubscribe_historical_bars(self):
    # Arrange
    self.client._eclient.reqHistoricalData = Mock()

    cancel_mock = Mock()
    self.client._eclient.cancelHistoricalData = cancel_mock

    # Act
    subscription = self.client.subscribe_bars(
        contract=Contract(),
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=Mock(),
    )
    subscription.cancel()

    # Assert
    cancel_mock.assert_called_once_with(reqId=-10)
    assert subscription not in self.client.subscriptions
