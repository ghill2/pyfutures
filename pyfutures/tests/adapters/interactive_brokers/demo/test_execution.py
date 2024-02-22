import asyncio

import pytest
from ibapi.order import Order

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import order_status_to_str
from nautilus_trader.model.events import OrderPendingCancel
from nautilus_trader.model.events import OrderPendingUpdate
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


async def _wait_for_order_status(order: Order, expected: OrderStatus):
    while order.status != expected:
        await asyncio.sleep(0)
        print(
            f"Waiting for status {order_status_to_str(expected)} for order {order.client_order_id}, current_status={order_status_to_str(order.status)}",
        )
        await asyncio.sleep(0.25)


class TestInteractiveBrokersExecutionFilled:
    @pytest.mark.asyncio()
    async def test_limit_order_filled(
        self,
        order_setup,
        cache,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=False,
        )

        await _wait_for_order_status(limit_order, OrderStatus.FILLED)

        assert limit_order.status == OrderStatus.FILLED

    @pytest.mark.asyncio()
    async def test_market_order_filled(
        self,
        order_setup,
        cache,
        instrument,
        delay,
        log,
    ):
        market_order = await order_setup.submit_market_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
        )

        await _wait_for_order_status(market_order, OrderStatus.FILLED)

        assert market_order.status == OrderStatus.FILLED


@pytest.mark.asyncio()
async def test_load_instrument_id(instrument_provider):
    instrument = await instrument_provider.load_async(InstrumentId.from_str("MIX.MEFFRV"))
    print(instrument)


class TestInteractiveBrokersExecutionCancelAccept:
    @pytest.mark.asyncio()
    async def test_limit_order_accepted(
        self,
        order_setup,
        cache,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        assert limit_order.status == OrderStatus.ACCEPTED

    @pytest.mark.asyncio()
    async def test_limit_order_cancel(
        self,
        order_setup,
        exec_client,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        cancel_order = CancelOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
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

        await _wait_for_order_status(limit_order, OrderStatus.CANCELED)

        assert limit_order.status == OrderStatus.CANCELED


class TestInteractiveBrokersExecutionSessions:
    @pytest.mark.asyncio()
    async def test_limit_order_modify_quantity_only(
        self,
        order_setup,
        exec_client,
        cache,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=Quantity.from_int(2),
            price=limit_order.price,
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

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

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        assert limit_order.status == OrderStatus.ACCEPTED
        assert limit_order.quantity == Quantity.from_int(2)

    @pytest.mark.asyncio()
    async def test_limit_order_modify_price_only(
        self,
        order_setup,
        exec_client,
        cache,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        new_price = Price(
            float(limit_order.price + instrument.price_increment),
            instrument.price_precision,
        )

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=limit_order.quantity,
            price=new_price,
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

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

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        assert limit_order.status == OrderStatus.ACCEPTED
        assert limit_order.price == new_price

    @pytest.mark.asyncio()
    async def test_limit_order_modify_price_and_quantity(
        self,
        order_setup,
        exec_client,
        cache,
        instrument,
        delay,
        log,
    ):
        limit_order = await order_setup.submit_limit_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument.id,
            quantity=Quantity.from_int(1),
            active=True,
        )

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        new_price = Price(
            float(limit_order.price + instrument.price_increment),
            instrument.price_precision,
        )

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=Quantity.from_int(2),
            price=new_price,
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

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

        await _wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        assert limit_order.status == OrderStatus.ACCEPTED
        assert limit_order.price == new_price
        assert limit_order.quantity == Quantity.from_int(2)
