import asyncio
from decimal import Decimal

import pytest
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from nautilus_trader.core.uuid import UUID4


"""
Tests to research the order of the api when orders are executed
"""


@pytest.mark.asyncio()
async def test_market_order_filled(client):
    """
    INFO:InteractiveBrokersClient:openOrder 15, orderStatus PreSubmitted, commission: 1.7976931348623157e+308, completedStatus:
    INFO:InteractiveBrokersClient:execDetails reqId=-1 ExecId: 0000e1a7.65e1eca3.01.01, Time: 20240301-19:09:16, Account: DU7779554, Exchange: CME, Side: BOT, Shares: 1, Price: 16.79, PermId: 1432478529, ClientId: 1, OrderId: 15, Liquidation: 0, CumQty: 1, AvgPrice: 16.79, OrderRef: f6e35614-1e4f-4cd1-a6b6-32ff344ca800, EvRule: , EvMultiplier: 0, ModelCode: , LastLiquidity: 1
    INFO:InteractiveBrokersClient:openOrder 15, orderStatus Filled, commission: 1.7976931348623157e+308, completedStatus:
    INFO:InteractiveBrokersClient:openOrder 15, orderStatus Filled, commission: 2.97USD, completedStatus:
    INFO:InteractiveBrokersClient:commissionReport
    """
    await client.connect()

    order = IBOrder()

    order.orderId = await client.request_next_order_id()
    order.orderRef = str(UUID4())
    order.orderType = "MKT"
    order.totalQuantity = Decimal("1")
    order.action = "BUY"
    order.tif = "GTC"

    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.secType = "FUT"
    contract.exchange = "CME"

    order.contract = await client.request_front_contract(contract)
    client.place_order(order)
    while True:
        await asyncio.sleep(0)


@pytest.mark.asyncio()
async def test_limit_order_accepted(client):
    """
    INFO:InteractiveBrokersClient:openOrder 14, orderStatus Submitted, commission: 1.7976931348623157e+308, completedStatus:
    """
    await client.connect()

    order = IBOrder()

    order.orderId = await client.request_next_order_id()
    order.orderRef = str(UUID4())
    order.orderType = "LMT"
    order.totalQuantity = Decimal("1")
    order.action = "BUY"
    order.tif = "GTC"

    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.secType = "FUT"
    contract.exchange = "CME"

    details = await client.request_front_contract_details(contract)
    contract = details.contract

    order.contract = contract

    quote = await client.request_last_quote_tick(contract)

    min_tick = details.minTick * details.priceMagnifier
    order.lmtPrice = quote.priceAsk - (min_tick * 1000)

    client.place_order(order)

    while True:
        await asyncio.sleep(0)
