import asyncio
import logging
import sys
from decimal import Decimal
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract
from ibapi.order import Order
from nautilus_trader.core.uuid import UUID4

from pyfutures.client.objects import ClientException


from pyfutures.tests.demo.client.stubs import ClientStubs

# REDUNDANT: reconnect tests this
# @pytest.mark.asyncio()
# async def test_reset(event_loop):
#     client = ClientStubs.client(loop=event_loop)
#     await client.reset()
#     await client.connect()


@pytest.mark.asyncio()
async def test_request_next_order_id(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.request_next_order_id()
    await client.request_next_order_id()


@pytest.mark.asyncio()
async def test_request_open_orders(event_loop):
    client = ClientStubs.client(loop=event_loop)
    orders = await asyncio.wait_for(client.request_open_orders(), 5)
    print(orders)


@pytest.mark.asyncio()
async def test_request_timezones(event_loop):
    pass


@pytest.mark.skip(reason="unused")
@pytest.mark.asyncio()
async def test_import_schedules(event_loop):
    pass


@pytest.mark.skip(reason="unused")
@pytest.mark.asyncio()
async def test_subscribe_account_updates(event_loop):
    client = ClientStubs.client(loop=event_loop)
    callback = Mock()

    await client.subscribe_account_updates(callback=callback)

    await asyncio.sleep(5)

    assert callback.call_count > 0


@pytest.mark.asyncio()
async def test_request_pnl(event_loop):
    return
