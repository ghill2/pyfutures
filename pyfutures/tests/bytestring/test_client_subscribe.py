import asyncio
from unittest.mock import Mock

import pytest

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.client.objects import ClientSubscription


@pytest.mark.asyncio()
async def test_subscribe_historical_bars(
    client,
    mode,
    qual_front_detail,
):
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
    sent_kwargs = send_mock.call_args_list[0][1]
    assert sent_kwargs == dict(
        reqId=-10,
        contract=qual_front_detail.contract,
        endDateTime="",
        durationStr="60 S",
        barSizeSetting="1 min",
        whatToShow="BID",
        useRTH=0,
        formatDate=2,
        keepUpToDate=True,
        chartOptions=[],
    )

    # wait until 3 bars have come in
    while callback_mock.call_count < 3:
        await asyncio.sleep(0)
    subscription.cancel()

    cancel_mock.assert_called_once_with(reqId=-10)
    assert subscription not in client._subscriptions.values()

    assert callback_mock.call_count == 3
