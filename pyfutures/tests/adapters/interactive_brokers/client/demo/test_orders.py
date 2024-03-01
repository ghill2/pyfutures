from decimal import Decimal
import asyncio
import pytest
from ibapi.order import Order as IBOrder
from nautilus_trader.core.uuid import UUID4
from ibapi.contract import Contract as IBContract

@pytest.mark.asyncio()
async def test_market_message_order(client):
    
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
async def test_limit_message_order(client):
    
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
    
    quote = await client.request_last_quote_tick(contract)
    print(quote)
    exit()
    # order.contract = 
    
    
    # client.place_order(order)
    
    while True:
        await asyncio.sleep(0)
    