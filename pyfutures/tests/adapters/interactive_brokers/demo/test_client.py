import asyncio
import time
from decimal import Decimal
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order

from nautilus_trader.core.uuid import UUID4
from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs


class TestInteractiveBrokersClient:
    @pytest.mark.asyncio()
    async def test_connect(self, client):
        while True:
            await asyncio.sleep(0)
            if client.is_connected:
                return True

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
        summary = await client.request_account_summary()
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
    async def test_request_head_timestamp_single(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        timestamp = await client.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID,
        )

        assert str(timestamp) == "2022-03-29 08:00:00+00:00"

    @pytest.mark.skip()
    @pytest.mark.asyncio()
    async def test_request_head_timestamp_universe(self, client):
        for contract in IBTestProviderStubs.universe_contracts():
            timestamp = await client.request_head_timestamp(
                contract=contract,
                what_to_show=WhatToShow.BID,
            )
            if timestamp is None:
                print(
                    f"No head timestamp for {contract.symbol} {contract.exchange} {contract.lastTradeDateOrContractMonth} {contract.conId}",
                )
                continue
            else:
                print(
                    f"Head timestamp found: {timestamp}  {contract.symbol} {contract.exchange} {contract.lastTradeDateOrContractMonth} {contract.conId}",
                )

            time.sleep(1)

    @pytest.mark.asyncio()
    async def test_request_historical_schedule(self):
        pass

    @pytest.mark.asyncio()
    async def test_request_quote_ticks(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        quotes = await asyncio.wait_for(
            client.request_quote_ticks(
                name="test",
                contract=contract,
                count=50,
            ),
            2,
        )

        assert len(quotes) == 54
        assert all(isinstance(quote, IBQuoteTick) for quote in quotes)

    @pytest.mark.skip(reason="trade ticks return 0 for this contract")
    @pytest.mark.asyncio()
    async def test_request_trade_ticks(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        trades = await asyncio.wait_for(
            client.request_trade_ticks(
                name="test",
                contract=contract,
                count=50,
            ),
            2,
        )

        assert len(trades) == 51
        assert all(isinstance(trade, IBTradeTick) for trade in trades)

    @pytest.mark.skip(reason="flakey if market not open")
    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks(self, client):
        callback_mock = Mock()

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_quote_ticks(
            name="test",
            contract=contract,
            callback=callback_mock,
        )

        async def wait_for_quote_tick():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_quote_tick(), 2)

        assert callback_mock.call_count > 0

    @pytest.mark.asyncio()
    async def test_request_bars(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        bars = await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_DAY,
            duration=Duration(4, Frequency.DAY),
            what_to_show=WhatToShow.BID,
        )

        assert all(isinstance(bar, IBBar) for bar in bars)
        assert len(bars) > 0

    @pytest.mark.skip(reason="flakey if market not open")
    @pytest.mark.asyncio()
    async def test_subscribe_bars_realtime(self, client):
        callback_mock = Mock()

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_bars(
            name="test",
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=callback_mock,
        )

        async def wait_for_bar():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_bar(), 2)

        assert callback_mock.call_count > 0

    @pytest.mark.skip(reason="flakey if market not open")
    @pytest.mark.asyncio()
    async def test_subscribe_bars_historical(self, client):
        callback_mock = Mock()

        client.bar_events += callback_mock

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_bars(
            name="test",
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._15_SECOND,
            callback=callback_mock,
        )

        async def wait_for_bar():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_bar(), 2)

        assert callback_mock.call_count > 0

    @pytest.mark.asyncio()
    async def test_request_accounts(self, client):
        await client.request_accounts()

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
