import asyncio
from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.adapters.interactive_brokers.data import (
    InteractiveBrokersDataClient,
)
from nautilus_trader.adapters.interactive_brokers.execution import (
    InteractiveBrokersExecutionClient,
)


class OrderSetup:
    def __init__(
        self,
        exec_client: InteractiveBrokersExecutionClient,
        data_client: InteractiveBrokersDataClient | None,
    ):
        self.cache = exec_client._cache
        self.trader_id = TestIdStubs.trader_id()
        self.strategy_id = TestIdStubs.strategy_id()

        self.exec_client = exec_client
        self.client = exec_client._client
        self.data_client = data_client
        self._instrument_provider = exec_client.instrument_provider

    async def submit_market_order(
        self,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        quantity: Quantity = None,
    ) -> MarketOrder:
        instrument = self._instrument_provider.find(instrument_id=instrument_id)
        assert instrument is not None

        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
            order_side=order_side,
            quantity=quantity or Quantity.from_int(1),
            client_order_id=ClientOrderId(str(self.client._next_req_id())),
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
        )

        self.cache.add_order(market_order)

        submit_order = SubmitOrder(
            trader_id=market_order.trader_id,
            strategy_id=market_order.strategy_id,
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )

        await self.exec_client._submit_order(submit_order)

        await asyncio.sleep(0)

        return market_order

    async def submit_limit_order(
        self,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        quantity: Quantity,
        price: Price = None,
        active: bool = True,
    ) -> LimitOrder:
        instrument = self._instrument_provider.find(instrument_id=instrument_id)
        assert instrument is not None

        if price is None:
            if active:
                # determine active limit order price
                last_quote = await self.exec_client.request_last_quote_tick(
                    instrument.id
                )
                if order_side is OrderSide.BUY:
                    price: Decimal = last_quote.ask_price - (
                        instrument.price_increment * 1000
                    )
                elif order_side is OrderSide.SELL:
                    price: Decimal = last_quote.bid_price + (
                        instrument.price_increment * 1000
                    )
            else:
                # determine immediate fill limit order price
                last_quote = await self.exec_client.request_last_quote_tick(
                    instrument.id
                )
                if order_side == OrderSide.BUY:
                    price: Decimal = last_quote.ask_price + (
                        instrument.price_increment * 50
                    )
                elif order_side == OrderSide.SELL:
                    price: Decimal = last_quote.bid_price - (
                        instrument.price_increment * 50
                    )

            if price <= 0:
                price = instrument.price_increment

            price = Price(price, instrument.price_precision)

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=order_side,
            quantity=quantity,
            client_order_id=ClientOrderId(
                str(self.client._next_req_id())
            ),
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
            price=price,
        )

        self.cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        await self.exec_client._submit_order(submit_order)

        await asyncio.sleep(0)

        return limit_order

    async def close_all(self) -> None:
        await self._close_all_positions()

    async def close_positions_for_instrument(self, instrument_id: InstrumentId) -> None:
        reports = [
            report
            for report in await self.exec_client.generate_position_status_reports()
            if report.instrument_id == instrument_id
        ]
        for report in reports:
            await self._close_position(report)

    async def _close_all_positions(self) -> None:
        for report in await self.exec_client.generate_position_status_reports():
            await self._close_position(report)
        self.client._client.reqGlobalCancel()

    async def _close_position(self, report) -> None:
        market_order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=report.instrument_id,
            client_order_id=ClientOrderId(str(self.client._next_req_id())),
            order_side=Order.closing_side(report.position_side),
            quantity=report.quantity,
            init_id=UUID4(),
            ts_init=0,
            time_in_force=TimeInForce.GTC,
        )
        self.cache.add_order(market_order)
        submit_order = SubmitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )
        await self.exec_client._submit_order(submit_order)

    async def _cancel_active_limit_orders(self) -> None:
        # for instrument in self._exec_client._instrument_provider.list_all():
        # for order_side in (OrderSide.BUY, OrderSide.SELL):
        for report in await self.exec_client.generate_order_status_reports():
            if report.order_type != OrderType.LIMIT:
                continue

            print(f"Cancelling {report.instrument_id} {report.order_side}")

            await self.exec_client._cancel_all_orders(
                command=CancelAllOrders(
                    trader_id=TestIdStubs.trader_id(),
                    strategy_id=TestIdStubs.strategy_id(),
                    instrument_id=report.instrument_id,
                    client_id=self.exec_client.id,
                    command_id=UUID4(),
                    ts_init=0,
                    order_side=report.order_side,
                ),
            )
            await asyncio.sleep(0.05)
