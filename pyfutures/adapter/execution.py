import asyncio

import pandas as pd
from ibapi.common import HistoricalTickBidAsk
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders.base import Order

from pyfutures.adapter import IB_VENUE
from pyfutures.adapter.parsing import AdapterParser
from pyfutures.adapter.parsing import order_side_to_order_action
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider
from pyfutures.client.client import IBErrorEvent
from pyfutures.client.client import IBExecutionEvent
from pyfutures.client.client import IBOpenOrderEvent
from pyfutures.client.client import IBPositionEvent
from pyfutures.client.client import InteractiveBrokersClient


class InteractiveBrokersExecClient(LiveExecutionClient):
    SUBMITTED_STATUSES = "Submitted"

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: InteractiveBrokersClient,
        account_id: AccountId,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InteractiveBrokersInstrumentProvider,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(IB_VENUE.value),
            venue=IB_VENUE,
            oms_type=OmsType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.MARGIN,
            base_currency=None,  # IB accounts are multi-currency
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            # config=InteractiveBrokersExecClientConfig(
            # name=f"{type(self).__name__}-{ibg_client_id:03d}",
            # ibg_client_id=1,
            # ),
        )

        self._client: InteractiveBrokersClient = client

        self._set_account_id(account_id)
        # self._account_summary_loaded: asyncio.Event = asyncio.Event()

        # self._client.order_status_events += self.order_status_callback
        self._client.error_events += self.error_callback
        self._client.execution_events += self.execution_callback
        self._client.open_order_events += self.open_order_callback

        self._parser = AdapterParser()

        # self._log._is_bypassed = True

    @property
    def instrument_provider(self) -> InteractiveBrokersInstrumentProvider:
        return self._instrument_provider

    @property
    def client(self) -> InteractiveBrokersClient:
        return self._client

    @property
    def cache(self) -> Cache:
        return self._cache

    async def _connect(self):
        await self._client.connect()

    def open_order_callback(self, event: IBOpenOrderEvent):
        """
        order modify:
        need access to quantity and price to compare the new order with the previous order
        can only access the order's price and quantity from openOrder
        """
        if event.orderState.status == "Filled" and event.order.orderType == "MKT":
            self._log.debug(
                f"Ignoring open order filled event for MKT, order filled handled by executioin {event}"
            )
            return  # nothing to do

        client_order_id = ClientOrderId(str(event.order.orderRef))
        if (order := self._find_order(client_order_id)) is None:
            return  # nothing to do
        if (instrument := self._find_instrument(order.instrument_id)) is None:
            return  # nothing to do

        self._log.debug(f"Received {event}")

        is_modified_event = (
            event.orderState.status not in self.SUBMITTED_STATUSES
            and event.order.orderType == "LMT"
        )
        if is_modified_event:
            # events related to orders that have been modified
            self._handle_open_order_modified(event)
        else:
            # new orders
            self._handle_open_order(event)

    def _handle_open_order(self, event: IBOpenOrderEvent) -> None:
        self._log.debug("Handling open order event")

        if event.orderState.status not in self.SUBMITTED_STATUSES:
            return  # nothing to do

        venue_order_id = VenueOrderId(str(event.order.orderId))
        client_order_id = ClientOrderId(str(event.order.orderRef))
        order = self._cache.order(client_order_id)

        """
        TODO: do these need to be matched?
        event.status == "PreSubmitted"
        event.status == "ApiPending"
        event.status == "PendingSubmit"
        """

        self._log.debug("generate_order_accepted")
        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

    def _handle_open_order_modified(self, event: IBOpenOrderEvent) -> None:
        self._log.debug("Handling open order event")

        client_order_id = ClientOrderId(str(event.order.orderRef))

        order = self._cache.order(client_order_id)

        total_qty = Quantity.from_str(str(event.order.totalQuantity))

        instrument = self._cache.instrument(order.instrument_id)
        price = instrument.make_price(event.order.lmtPrice)

        if total_qty == order.quantity and price == order.price:
            return  # no change

        venue_order_id = VenueOrderId(str(event.order.orderId))
        venue_order_id_modified = (
            order.venue_order_id is None or order.venue_order_id != venue_order_id
        )

        self._log.debug("generate_order_updated")

        self.generate_order_updated(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            quantity=total_qty,
            price=price,
            trigger_price=None,
            ts_event=self._clock.timestamp_ns(),
            venue_order_id_modified=venue_order_id_modified,
        )

    async def _submit_order(self, command: SubmitOrder) -> None:
        PyCondition.type(command, SubmitOrder, "command")

        order = command.order

        # reject fractional quantity, all universe instruments have a size increment of 1
        if order.order_type == OrderType.LIMIT and not (
            order.quantity.as_double() % 1 == 0
        ):
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        # reject negative and zero price
        if order.order_type == OrderType.LIMIT and order.price.as_double() <= 0.0:
            self._log.error(f"Order received with 0 price {order}")
            return

        if (instrument := self._find_instrument(order.instrument_id)) is None:
            return  # nothing to do

        ib_order: Order = self._parser.nautilus_order_to_ib_order(
            order=command.order,
            instrument=instrument,
            order_id=await self._client.request_next_order_id(),
        )

        self._log.info(f"Submitting order {ib_order}...")

        self._client.place_order(ib_order)

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _modify_order(self, command: ModifyOrder) -> None:
        PyCondition.not_none(command, "command")

        if not (command.quantity.as_double() % 1 == 0):
            self.generate_order_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            return  # reject fractional quantity
        if command.price.as_double() <= 0.0:
            self._log.error(f"Order received with 0 price {command}")
            return  # reject negative or zero price
        if (order := self._find_order(command.client_order_id)) is None:
            return  # nothing to do
        if (instrument := self._find_instrument(order.instrument_id)) is None:
            return  # nothing to do

        self._log.debug(f"Modifying order {command.client_order_id}")

        ib_order: IBOrder = self._parser.nautilus_order_to_ib_order(
            order=order,
            instrument=instrument,
            order_id=await self._client.request_next_order_id(),
        )

        # update order attributes to the the desired modification
        if command.quantity and command.quantity != ib_order.totalQuantity:
            ib_order.totalQuantity = command.quantity.as_double()
        if command.price and command.price.as_double() != getattr(
            ib_order, "lmtPrice", None
        ):
            ib_order.lmtPrice = command.price.as_double()

        # set parentId
        if ib_order.parentId == "":
            parent_nautilus_order = self._cache.order(ClientOrderId(ib_order.parentId))
            if parent_nautilus_order is not None:
                ib_order.parentId = int(parent_nautilus_order.venue_order_id.value)
            else:
                ib_order.parentId = 0

        self._log.info(f"Placing {ib_order!r}")

        self._client.place_order(ib_order)

    async def _cancel_order(self, command: CancelOrder) -> None:
        PyCondition.not_none(command, "command")

        # TODO: search cache for order first?
        # order = self._cache.order(command.client_order_id)
        # if order is None:
        #     self._log.debug(f"No order found in cache")

        # TODO: search cache for order with venue order id first?

        self._client.cancel_order(int(command.venue_order_id.value))

    def execution_callback(self, event: IBExecutionEvent):
        self._log.debug(f"IBExecutionEvent received: {event}")

        client_order_id = ClientOrderId(str(event.execution.orderRef))

        if (order := self._find_order(client_order_id)) is None:
            return  # nothing to do
        if (instrument := self._find_instrument(order.instrument_id)) is None:
            return  # nothing to do

        self._execution_callback(event)

    def _execution_callback(self, event: IBExecutionEvent):
        client_order_id = ClientOrderId(str(event.execution.orderRef))

        order = self._cache.order(client_order_id)

        instrument = self._cache.instrument(order.instrument_id)

        self._log.debug("generate_order_filled")
        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId(str(event.execution.orderId)),
            venue_position_id=None,
            trade_id=TradeId(event.execution.execId),
            order_side=OrderSide[order_side_to_order_action[event.execution.side]],
            order_type=order.order_type,
            last_qty=Quantity(
                event.execution.shares, precision=instrument.size_precision
            ),
            last_px=Price(event.execution.price, precision=instrument.price_precision),
            quote_currency=instrument.quote_currency,
            commission=Money(
                event.commissionReport.commission,
                Currency.from_str(event.commissionReport.currency),
            ),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,  # TODO: LIQUIDITY_SIDE.TAKER?
            ts_event=dt_to_unix_nanos(event.timestamp),
        )

        # self._log.debug(f"OrderStatus is {order.status!r}")
        # assert order.status == OrderStatus.FILLED

    def _find_instrument(self, instrument_id: InstrumentId) -> Instrument | None:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.error(
                f"Instrument not found in instrument provider {instrument_id}"
            )
        return instrument

    def _find_order(self, client_order_id: ClientOrderId) -> Order | None:
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.error(f"Order not found in cache with: {client_order_id}")
        return order

    def error_callback(self, event: IBErrorEvent) -> None:
        """
        # 201: This order does not comply with our order handling rules for derivatives subject to IBKR near-expiration and<br>physical delivery risk policies.<br>Please refer to our <a href="https://www.interactivebrokers.com/en/index.php?f=1567&p=physical">website</a> for further details
        # --> Warning 202 req_id= Order Canceled - reason
        # 10318: This order doesn't support fractional quantity trading
        # 203: The security is not available or allowed for this account.
        IBErrorEvent(request_id=0, code=10318, message="This order doesn't support fractional quantity trading", advanced_order_reject_json='')
        IBErrorEvent(request_id=111, code=201, message='Order rejected - reason:YOUR ORDER IS NOT ACCEPTED. IN ORDER TO OBTAIN THE DESIRED POSITION YOUR NET LIQ [-16132592.69 GBP] MUST EXCEED THE MARGIN REQ [54635513425.67 GBP]', advanced_order_reject_json='')
        IBErrorEvent(request_id=9999999, code=10147, message='OrderId 9999999 that needs to be cancelled is not found.', advanced_order_reject_json='')
        IBErrorEvent(request_id=128, code=110, message='The price does not conform to the minimum price variation for this contract.', advanced_order_reject_json='')
        IBErrorEvent(request_id=166, code=161, message='Cancel attempted when order is not in a cancellable state.  Order permId =1334608417', advanced_order_reject_json='')
        IBErrorEvent(request_id=5905, code=2161, message="BUY 1 MDAX DEC'23 @ 26439.00  In accordance with our regulatory obligations as a broker, we will initially cap (or limit) the price of your Limit Order to 26270.00 or a more aggressive price still within your specified limit price.  If your order is not immediately executable, our systems may, depending on market conditions, cap your order to a limit price that is no more than the allowed amount away from the reference price at that time. If this happens, you will not receive a fill. This is a control designed to ensure that we comply with our regulatory obligations to avoid submitting disruptive orders to the marketplace. Please note that in some circumstances this may result in you receiving a less favorable fill or not receiving a fill.  In the future, please submit your order using a limit price that is closer to the current market price or submit an algorithmic Market Order (IBALGO).  If you would like to cancel your order, please use cancel order action.", advanced_order_reject_json='')
        """
        self._log.error(f"{event.reqId} {event!r}")
        # error_codes = [201, 202, 203, 10318]
        # if event.code not in error_codes:
        #     self._log.debug(f"Error code {event.code} was not in error codes, returning.")
        #     return

        # an error related to an order will have a positive request_id
        # all client request_ids are negative
        if event.reqId <= 0:
            self._log.debug(
                f"Error with code {event.errorCode} was an error NOT related to an order, doing nothing"
            )
            return

        venue_order_id = VenueOrderId(str(event.reqId))
        client_order_id = self._cache.client_order_id(venue_order_id)
        if client_order_id is None:
            self._log.debug(
                f"client_order_id not found for venue_order_id: {venue_order_id}"
            )
            return

        if (order := self._find_order(client_order_id)) is None:
            return  # nothing to do

        if event.errorCode == 202:
            self._log.error(
                f"Order with id {event.reqId} was canceled: {event.errorString}"
            )
            self._log.debug("generate_order_canceled")
            # TODO: check for order.status == OrderStatus.PENDING_CANCEL?
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
        elif event.errorCode in [201, 10318, 110, 161, 2161]:
            if order.status == OrderStatus.PENDING_UPDATE:
                self._log.debug("generate_modify_rejected")
                self.generate_order_modify_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    reason=event.errorString,
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                self._log.debug("generate_order_rejected")
                # TODO: check for order.status == OrderStatus.SUBMITTED?
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=event.errorString,
                    ts_event=self._clock.timestamp_ns(),
                )

    async def generate_trade_reports(
        self,
        instrument_id: InstrumentId | None = None,
        venue_order_id: VenueOrderId | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> list[FillReport]:
        self._log.warning("Cannot generate `FillReports`: not yet implemented.")

        return []

    async def generate_position_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> list[PositionStatusReport]:
        reports = []

        positions: list[IBPositionEvent] = await self._client.request_positions()

        for position in positions:
            self._log.debug(f"Trying PositionStatusReport for {position.conId}")

            if position.quantity > 0:
                side = PositionSide.LONG
            elif position.quantity < 0:
                side = PositionSide.SHORT
            else:
                continue  # Skip, IB may continue to display closed positions

            instrument = await self._cache.instrument_with_contract_id(
                position.conId,
            )

            report = PositionStatusReport(
                account_id=self.account_id,
                instrument_id=instrument.id,
                position_side=side,
                quantity=Quantity.from_str(str(abs(position.quantity))),
                report_id=UUID4(),
                ts_last=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
            )

            self._log.debug(f"Received {report!r}")
            reports.append(report)

        return reports

    async def generate_order_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        open_only: bool = False,
    ) -> list[OrderStatusReport]:
        reports = []

        events: list[IBOpenOrderEvent] = await self._client.request_open_orders()
        for event in events:
            instrument = await self._cache.instrument_with_contract_id(
                event.conId,
            )

            report = self._parser.order_event_to_order_status_report(
                instrument=instrument,
                event=event,
                now_ns=self._clock.timestamp_ns(),
                account_id=self.account_id,
            )
            self._log.debug(f"Received {report!r}")
            reports.append(report)

        return reports

    async def generate_order_status_report(
        self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId | None = None,
        venue_order_id: VenueOrderId | None = None,
    ) -> OrderStatusReport | None:
        PyCondition.type_or_none(client_order_id, ClientOrderId, "client_order_id")
        PyCondition.type_or_none(venue_order_id, VenueOrderId, "venue_order_id")
        if not (client_order_id or venue_order_id):
            self._log.debug(
                "Both `client_order_id` and `venue_order_id` cannot be None."
            )
            return None

        report = None

        events: list[IBOpenOrderEvent] = await self._client.request_open_orders()

        for event in events:
            if (
                client_order_id is not None and client_order_id.value == event.orderRef
            ) or (
                venue_order_id is not None
                and venue_order_id.value == str(event.orderId)
            ):
                instrument = await self._cache.instrument_with_contract_id(event.conId)

                report = self._parser.order_event_to_order_status_report(
                    instrument=instrument,
                    event=event,
                    now_ns=self._clock.timestamp_ns(),
                    account_id=self.account_id,
                )

        return report

    async def request_last_quote_tick(
        self, instrument_id: InstrumentId
    ) -> HistoricalTickBidAsk:
        self._log.debug(f"Requesting last quote tick for {instrument_id}")

        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.error(f"No instrument found for {instrument_id}")
            return

        contract = IBContract()
        contract.conId = instrument.info["contract"]["conId"]
        contract.exchange = instrument.info["contract"]["exchange"]

        last_quote = await self._client.request_last_quote_tick(contract=contract)

        return self._parser.historical_tick_to_nautilus_quote_tick(
            instrument=instrument, tick=last_quote
        )


# generate modify
# total_qty = (
#     Quantity.from_int(0)
#     if event.totalQuantity == UNSET_DECIMAL
#     else Quantity.from_str(str(event.totalQuantity))
# )
# price = (
#     None if event.lmtPrice == UNSET_DOUBLE else instrument.make_price(event.lmtPrice)
# )

# def order_status_callback(self, event: IBOrderStatusEvent) -> None:

#     self._log.info(f"OrderStatusEvent received: {event}")

#     client_order_id = ClientOrderId(str(event.order_id))

#     self._log.info(f"client_order_id: {client_order_id}")

#     order = self._cache.order(client_order_id)

#     if order is None:
#         self._log.info(f"No order found for client_order_id: {event.order_id}")
#         return

#     self._log.info(f"order: {order}")

#     venue_order_id = VenueOrderId(str(event.order_id))
#     self._log.info(repr(venue_order_id))
#     self._log.info(event.status)

#     self._log.debug(str(event.status == "Submitted"))
#     if event.status == "Submitted" \
#         or event.status == "PreSubmitted" \
#         or event.status == "ApiPending" \
#         or event.status == "PendingSubmit":

#         self._log.debug("generate_order_accepted")
#         self.generate_order_accepted(
#             strategy_id=order.strategy_id,
#             instrument_id=order.instrument_id,
#             client_order_id=order.client_order_id,
#             venue_order_id=venue_order_id,
#             ts_event=self._clock.timestamp_ns(),
#         )

# elif event.status == "ApiCancelled" or event.status == "Cancelled":

#     self._log.debug("generate_order_canceled")
#     self.generate_order_canceled(
#         strategy_id=order.strategy_id,
#         instrument_id=order.instrument_id,
#         client_order_id=order.client_order_id,
#         venue_order_id=order.venue_order_id,
#         ts_event=self._clock.timestamp_ns(),
#     )
