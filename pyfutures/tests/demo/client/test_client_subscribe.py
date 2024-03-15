import asyncio
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract as IBContract
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow


from pyfutures.tests.demo.client.stubs import ClientStubs


@pytest.mark.asyncio()
async def test_subscribe_quote_ticks(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    callback_mock = Mock()

    contract = IBContract()
    # contract.conId = 553444806
    # contract.exchange = "ICEEUSOFT"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"
    contract.symbol = "EUR"
    contract.currency = "GBP"

    client.subscribe_quote_ticks(
        contract=contract,
        callback=callback_mock,
    )

    async def wait_for_quote_tick():
        while callback_mock.call_count == 0:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_quote_tick(), 10)

    assert callback_mock.call_count > 0


# @pytest.mark.skip(reason="flakey if market not open")
@pytest.mark.asyncio()
async def test_subscribe_realtime_bars(event_loop):
    client = ClientStubs.client(loop=event_loop)
    client.request_market_data_type(4)
    callback_mock = Mock()

    contract = IBContract()
    # contract.conId = 553444806
    # contract.exchange = "ICEEUSOFT"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"
    contract.symbol = "EUR"
    contract.currency = "GBP"

    client._subscribe_realtime_bars(
        contract=contract,
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._5_SECOND,
        callback=callback_mock,
    )

    async def wait_for_bar():
        while callback_mock.call_count == 0:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_bar(), 20)

    assert callback_mock.call_count > 0

    # TODO: do we need this test?
    # @pytest.mark.skip(reason="flakey if market not open")
    # @pytest.mark.asyncio()
    # async def test_subscribe_bars_historical(self, client):
    #     callback_mock = Mock()
    #
    #     client.bar_events += callback_mock
    #
    #     contract = Contract()
    #     contract.conId = 553444806
    #     contract.exchange = "ICEEUSOFT"
    #
    #     client.subscribe_bars(
    #         name="test",
    #         contract=contract,
    #         what_to_show=WhatToShow.BID,
    #         bar_size=BarSize._15_SECOND,
    #         callback=callback_mock,
    #     )
    #
    #     async def wait_for_bar():
    #         while callback_mock.call_count == 0:
    #             await asyncio.sleep(0)
    #
    #     await asyncio.wait_for(wait_for_bar(), 2)
    #
    #     assert callback_mock.call_count > 0
    #
