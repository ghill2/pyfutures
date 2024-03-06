import asyncio
from decimal import Decimal
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order
from nautilus_trader.core.uuid import UUID4

from pyfutures.client.objects import ClientException

import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestInteractiveBrokersClient:
    @pytest.mark.asyncio()
    async def test_reset(self, client):
        await client.reset()
        await client.connect()

    @pytest.mark.asyncio()
    async def test_request_contract_details_returns_expected(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"
        contract.includeExpired = True

        results = await client.request_contract_details(contract)
        print(results)
        assert isinstance(results, list)
        assert all(isinstance(result, IBContractDetails) for result in results)

    @pytest.mark.asyncio()
    async def test_request_contract_details_raises_exception(self, client):
        contract = Contract()
        contract.secType = "invalid_secType"
        contract.symbol = "D"
        contract.exchange = "ICEEUSOFT"

        with pytest.raises(ClientException) as e:
            await client.request_contract_details(contract)
            assert e.code == 321

    @pytest.mark.asyncio()
    async def test_request_account_summary(self, client):
        await client.connect()
        summary = await client.request_account_summary()
        print(summary)
        assert isinstance(summary, dict)

    @pytest.mark.asyncio()
    async def test_request_next_order_id(self, client):
        await client.request_next_order_id()
        await client.request_next_order_id()

    @pytest.mark.skip(reason="can fail due to too many orders")
    @pytest.mark.asyncio()
    async def test_place_market_order(self, client):
        contract = Contract()
        contract.conId = 564400671
        contract.exchange = "ICEEUSOFT"

        order = Order()
        order.contract = contract

        # MARKET order
        order.orderId = await client.request_next_order_id()
        order.orderRef = str(UUID4())  # client_order_id
        order.orderType = "MKT"  # order_type
        order.totalQuantity = Decimal(1)  # quantity
        order.action = "BUY"  # side

        client.place_order(order)

    @pytest.mark.skip(reason="can fail due to too many orders")
    @pytest.mark.asyncio()
    async def test_limit_place_order(self, client):
        contract = Contract()
        contract.conId = 564400671
        contract.exchange = "ICEEUSOFT"

        order = Order()
        order.contract = contract

        # LIMIT order
        order.orderId = await client.request_next_order_id()
        order.orderRef = str(UUID4())  # client_order_id
        order.orderType = "LMT"  # order_type
        order.totalQuantity = Decimal(1)  # quantity
        order.action = "BUY"  # side
        order.lmtPrice = 2400.0  # price
        order.tif = "GTC"  # time in force

        client.place_order(order)

    @pytest.mark.asyncio()
    async def test_request_open_orders(self, client):
        orders = await asyncio.wait_for(client.request_open_orders(), 5)
        print(orders)

    @pytest.mark.asyncio()
    async def test_request_positions(self, client):
        positions = await asyncio.wait_for(client.request_positions(), 5)
        print(positions)

    @pytest.mark.skip(reason="large amount of order infinite loop")
    @pytest.mark.asyncio()
    async def test_request_executions(self, client):
        executions = await asyncio.wait_for(client.request_executions(), 5)

        print(executions)

    @pytest.mark.asyncio()
    async def test_request_accounts(self, client):
        await client.request_accounts()

    @pytest.mark.asyncio()
    async def test_request_historical_schedule(self, client):
        await client.connect()

        contract = Contract()
        contract.symbol = "SCI"
        contract.localSymbol = "FEFF27"
        contract.exchange = "SGX"
        contract.secType = "FUT"
        contract.includeExpired = False
        df = await client.request_historical_schedule(contract=contract)
        print(df.iloc[:49])

    @pytest.mark.asyncio()
    async def test_request_timezones(self, client):
        pass

    @pytest.mark.skip(reason="unused")
    @pytest.mark.asyncio()
    async def test_import_schedules(self, client):
        pass

    @pytest.mark.skip(reason="unused")
    @pytest.mark.asyncio()
    async def test_subscribe_account_updates(self, client):
        callback = Mock()

        await client.subscribe_account_updates(callback=callback)

        await asyncio.sleep(5)

        assert callback.call_count > 0

    @pytest.mark.asyncio()
    async def test_request_portfolio(self, client):
        await client.request_portfolio()

    @pytest.mark.asyncio()
    async def test_request_pnl(self, client):
        return

    @pytest.mark.asyncio()
    async def test_request_market_data_type_returns_expected(self, client):
        expected_market_data_type = 4

        await client.connect()
        # market_data_type = await client.request_market_data_type(
        #     market_data_type=expected_market_data_type
        # )
        # assert expected_market_data_type == market_data_type
