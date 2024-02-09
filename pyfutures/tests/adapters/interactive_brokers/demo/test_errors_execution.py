import asyncio

import pytest

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderPendingCancel
from nautilus_trader.model.events import OrderPendingUpdate
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class TestExecutionErrors:
    @pytest.mark.asyncio()
    async def test_limit_order_modify_rejected_invalid_quantity(
        self,
        order_setup,
        exec_client,
        cache,
    ):
        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument_id,
            quantity=Quantity.from_int(1),
            price=Price.from_str("87.68"),
            active=True,
        )

        print(limit_order.client_order_id)
        print(limit_order.price)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        # previous_price = limit_order.price

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=Quantity.from_str("10000000"),
            price=limit_order.price,
            # price=Price.from_int(0.0000001),  # invalid price
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

        # exec client doesn't modify the order's status to PENDING_UPDATE
        limit_order.apply(
            OrderPendingUpdate(
                trader_id=limit_order.trader_id,
                strategy_id=limit_order.strategy_id,
                instrument_id=limit_order.instrument_id,
                client_order_id=limit_order.client_order_id,
                venue_order_id=None,
                account_id=None,
                event_id=UUID4(),
                ts_event=0,
                ts_init=0,
                reconciliation=False,
            ),
        )

        assert limit_order.status == OrderStatus.PENDING_UPDATE
        await exec_client._modify_order(modify_order)

        await asyncio.sleep(1)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.status == OrderStatus.ACCEPTED  # no change

    @pytest.mark.skip(
        reason="can't seem to create this case, ib accepts any price above 0"
    )
    @pytest.mark.asyncio()
    async def test_limit_order_canceled_invalid_price(
        self,
        order_setup,
        cache,
    ):
        """
        A limit order with an invalid price is accepted then canceled, vs just rejected.
        """
        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument_id,
            quantity=Quantity.from_int(1),
            price=Price.from_str("100000000"),
        )

        await self._wait_for_order_status(limit_order, OrderStatus.CANCELED)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.status == OrderStatus.CANCELED

    @pytest.mark.asyncio()
    async def test_limit_order_cancel_rejected(
        self,
        order_setup,
        exec_client,
    ):
        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument_id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        print(limit_order.client_order_id)
        print(limit_order.price)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        cancel_order = CancelOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=ClientOrderId("9999999"),
            venue_order_id=VenueOrderId("9999999"),  # invalid venue_order_id
            command_id=UUID4(),
            ts_init=0,
        )

        limit_order.apply(
            OrderPendingCancel(
                trader_id=limit_order.trader_id,
                strategy_id=limit_order.strategy_id,
                instrument_id=limit_order.instrument_id,
                client_order_id=limit_order.client_order_id,
                venue_order_id=None,
                account_id=None,
                event_id=UUID4(),
                ts_event=0,
                ts_init=0,
                reconciliation=False,
            ),
        )

        assert limit_order.status == OrderStatus.PENDING_CANCEL

        await exec_client._cancel_order(cancel_order)

        await self._wait_for_order_status(limit_order, OrderStatus.CANCELED)

    @pytest.mark.asyncio()
    async def test_market_order_rejected_invalid_quantity(
        self,
        order_setup,
        cache,
    ):
        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        market_order = await order_setup.submit_market_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument_id,
            quantity=Quantity.from_str("0.5"),  # invalid quantity
        )

        await self._wait_for_order_status(market_order, OrderStatus.REJECTED)

        cached_order = cache.order(market_order.client_order_id)

        assert cached_order is not None
        assert cached_order.status == OrderStatus.REJECTED
