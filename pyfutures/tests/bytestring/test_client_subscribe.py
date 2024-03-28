import pandas as pd
import pytest
from ibapi.common import BarData
from ibapi.contract import Contract
from unittest.mock import AsyncMock
from unittest.mock import Mock
from pyfutures.client.objects import ClientSubscription

import asyncio

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow


@pytest.mark.asyncio()
async def test_subscribe_historical_bars(client, mode, qual_front_detail):
    await client.connect()
    send_mock = Mock(side_effect=client._eclient.reqHistoricalData)
    client._eclient.reqHistoricalData = send_mock

    cancel_mock = Mock(side_effect=client._eclient.cancelHistoricalData)
    client._eclient.cancelHistoricalData = cancel_mock

    # def on_bar(bar):
    #     assert isinstance(bar, BarData)

    callback_mock = Mock()

    # Act
    subscription = client.subscribe_bars(
        contract=qual_front_detail.contract,
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=callback_mock,
    )

    assert isinstance(subscription, ClientSubscription)

    # Assert
    # sent_kwargs = send_mock.call_args_list[0][1]
    # assert sent_kwargs == dict(
    #     reqId=-10,
    #     contract=qual_front_detail.contract,
    #     endDateTime="",
    #     durationStr="30 S",
    #     barSizeSetting="30 secs",
    #     whatToShow="BID",
    #     useRTH=0,
    #     formatDate=2,
    #     keepUpToDate=True,
    #     chartOptions=[],
    # )

    # wait until 3 bars have come in
    # while callback_mock.call_count < 3:
    # await asyncio.sleep(0)

    subscription.cancel()
    print(callback_mock.call_count)

    # wait to see if any other bars come in after the cancellation
    # in unit mode the
    # if mode != "unit":
    # await asyncio.sleep(20)

    # Assert
    # assert bar.timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")

    cancel_mock.assert_called_once_with(reqId=-10)
    assert subscription not in client._subscriptions.values()

    print("WAITING 10 secs")
    await asyncio.sleep(10)
