import pytest
from pytower.stats.whatif import sort_by_whatif_value
from pyfutures.tests.test_kit import IBTestProviderStubs
import pandas as pd
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from nautilus_trader.core.uuid import UUID4
import pytest_asyncio


@pytest.fixture
def contract(client):
    """
    lowest margin with open trading hours contract 27/03/2024 09:50+00:00
    """
    contract = IBContract()
    contract.tradingClass = "EBM"
    contract.symbol = "EBM"
    contract.exchange = "MATIF"
    contract.secType = "FUT"
    return contract


# @pytest.mark.skip(reason="can fail due to too many orders")
@pytest.mark.asyncio()
async def test_market_order_filled(client, contract):
    await client.connect()
    detail = await client.request_front_contract_details(contract)

    order = IBOrder()
    order.contract = detail.contract

    req_id = await client.request_next_order_id()

    # MARKET order
    order.orderId = req_id
    order.orderRef = str(UUID4())  # client_order_id
    order.orderType = "MKT"  # order_type
    order.totalQuantity = detail.minSize
    order.action = "BUY"  # side
    order.tif = "GTC"

    client.place_order(order)
    await asyncio.sleep(2)
    print()


@pytest.mark.skip(reason="helper")
@pytest.mark.asyncio()
async def test_open_low_margin_instruments():
    """ """
    rows = IBTestProviderStubs.universe_rows()
    now = pd.Timestamp.utcnow()
    open_rows = []
    for row in rows:
        if row.liquid_schedule.is_open(now=now):
            open_rows.append(row)

    print(len(open_rows))
    print("open_rows[0]", open_rows[0])
    open_sorted_rows = await sort_by_whatif_value(open_rows, "initMarginChange")
    print("open_sorted_rows[0]", open_sorted_rows[0])
