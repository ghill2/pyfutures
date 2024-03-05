import asyncio
from unittest.mock import Mock
import pandas as pd

from decimal import Decimal
import pytest
from nautilus_trader.core.uuid import UUID4
from ibapi.order import Order as IBOrder

from ibapi.common import UNSET_DOUBLE
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.model.events.order import OrderAccepted
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.enums import OrderSide
from pyfutures.adapter.client.client import IBErrorEvent
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.currencies import GBP
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import order_status_to_str
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.commands import TestCommandStubs
from nautilus_trader.model.objects import Money
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.objects import Quantity
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs

from pyfutures.adapter.client.objects import IBOrderStatusEvent
from pyfutures.adapter.client.objects import IBOpenOrderEvent
from pyfutures.tests.unit.adapter.stubs.execution import IBTestExecutionStubs

from pyfutures.tests.unit.adapter.stubs.identifiers import IBTestIdStubs
from pyfutures.adapter.client.client import IBExecutionEvent

class TestInteractiveBrokersExecution:
    
    def setup_method(self):
        self.instrument_id = InstrumentId.from_str("MES=MES=2023Z.CME")
        
    @pytest.mark.asyncio()
    async def test_submit_market_order_sends_expected(
        self,
        exec_client,
    ):
        
        # Arrange
        market_order = TestExecStubs.market_order(
            instrument_id=self.instrument_id,
        )

        submit_order = SubmitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )
        
        exec_client._client.place_order = Mock()
        
        # Act
        await exec_client._submit_order(submit_order)
        
        # Assert
        exec_client._client.place_order.assert_called_once()
        sent_order = exec_client._client.place_order.call_args_list[0][0][0]
        assert sent_order.orderId == IBTestIdStubs.orderId()
        assert sent_order.orderRef == TestIdStubs.client_order_id().value
        assert sent_order.orderType == "MKT"
        assert sent_order.totalQuantity == Decimal("100")
        assert sent_order.action == "BUY"
        assert sent_order.tif == "GTC"
        assert sent_order.contract.conId == IBTestIdStubs.conId()
        assert sent_order.contract.exchange == "CME"
        assert sent_order.lmtPrice == UNSET_DOUBLE  # unset
    
    @pytest.mark.asyncio()
    async def test_submit_limit_order_sends_expected(
        self,
        exec_client,
    ):
        limit_order = TestExecStubs.limit_order(
            instrument_id=self.instrument_id,
        )
        
        # Arrange
        submit_order = SubmitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )
        
        exec_client._client.place_order = Mock()
        
        # Act
        await exec_client._submit_order(submit_order)
        
        # Assert
        exec_client._client.place_order.assert_called_once()
        sent_order = exec_client._client.place_order.call_args_list[0][0][0]
        assert sent_order.orderId == IBTestIdStubs.orderId()
        assert sent_order.orderRef == TestIdStubs.client_order_id().value
        assert sent_order.orderType == "LMT"
        assert sent_order.totalQuantity == Decimal("100")
        assert sent_order.action == "BUY"
        assert sent_order.tif == "GTC"
        assert sent_order.contract.conId == IBTestIdStubs.conId()
        assert sent_order.contract.exchange == "CME"
        assert sent_order.lmtPrice == 55.0
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_modify_order_sends_expected(
        self,
        exec_client,
    ):
        pass
        
    @pytest.mark.asyncio()
    async def test_order_submitted_sends_expected(
        self,
        exec_client,
    ):
        
        # Arrange
        instrument_id = self.instrument_id
        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
        )

        submit_order = SubmitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )
        exec_client.generate_order_submitted = Mock()
        
        # Act
        await exec_client._submit_order(submit_order)
        
        # Assert
        exec_client.generate_order_submitted.assert_called_once()
        submitted_kwargs = exec_client.generate_order_submitted.call_args_list[0][1]
        assert submitted_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert submitted_kwargs["instrument_id"] == instrument_id
        assert submitted_kwargs["client_order_id"] == TestIdStubs.client_order_id()
    
    def test_order_accepted_response(
        self,
        exec_client,
    ):
        # Arrange
        instrument_id = self.instrument_id
        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
        )
        exec_client.cache.add_order(market_order)
        exec_client.generate_order_accepted = Mock()
        
        # Act
        event: IBOpenOrderEvent = IBTestExecutionStubs.open_order_event()
        exec_client.open_order_callback(event)
        
        # Assert
        exec_client.generate_order_accepted.assert_called_once()
        accepted_kwargs = exec_client.generate_order_accepted.call_args_list[0][1]
        assert accepted_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert accepted_kwargs["instrument_id"] == instrument_id
        assert accepted_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert accepted_kwargs["venue_order_id"] == VenueOrderId(str(IBTestIdStubs.orderId()))
        
    def test_order_filled_response(
        self,
        exec_client,
    ):
        
        # Arrange
        instrument_id = self.instrument_id
        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
        )
        exec_client.cache.add_order(market_order)
        exec_client.generate_order_filled = Mock()
        
        # Act
        event: IBExecutionEvent = IBTestExecutionStubs.execution_event()
        exec_client.execution_callback(event)
        
        # Assert
        exec_client.generate_order_filled.assert_called_once()
        filled_kwargs = exec_client.generate_order_filled.call_args_list[0][1]
        assert filled_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert filled_kwargs["instrument_id"] == instrument_id
        assert filled_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert filled_kwargs["venue_order_id"] == VenueOrderId(str(IBTestIdStubs.orderId()))
        assert filled_kwargs["venue_position_id"] is None
        assert filled_kwargs["trade_id"] == TradeId("0000e9b5.6555a859.01.01")
        assert filled_kwargs["order_side"] == OrderSide.BUY
        assert filled_kwargs["order_type"] == OrderType.MARKET
        assert filled_kwargs["last_qty"] == Quantity.from_int(1)
        assert filled_kwargs["last_px"] == Price.from_str("1.2345")
        assert filled_kwargs["quote_currency"] == GBP
        assert filled_kwargs["commission"] == Money(1.2, GBP)
        assert filled_kwargs["liquidity_side"] == LiquiditySide.NO_LIQUIDITY_SIDE
        assert filled_kwargs["ts_event"] == dt_to_unix_nanos(pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC"))
    
    @pytest.mark.asyncio()
    async def test_cancel_order_sends_expected(
        self,
        exec_client,
    ):
        
        # Arrange
        cancel_order = CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=TestIdStubs.audusd_idealpro_id(),
            client_order_id=TestIdStubs.client_order_id(),
            venue_order_id=VenueOrderId(str(IBTestIdStubs.orderId())),
            command_id=UUID4(),
            ts_init=0,
        )
        
        exec_client._client.cancel_order = Mock()
        
        # Act
        await exec_client._cancel_order(cancel_order)
        
        # Assert
        exec_client._client.cancel_order.assert_called_once_with(
            IBTestIdStubs.orderId(),
        )
        
    @pytest.mark.asyncio()
    async def test_modify_order_sends_expected(
        self,
        exec_client,
    ):
            
        # Arrange
        instrument = TestInstrumentProvider.default_fx_ccy("AUD/USD", venue=Venue("SIM"))
        exec_client.instrument_provider.add(instrument)
            
        
        limit_order = TestExecStubs.limit_order(
            instrument_id=self.instrument_id,
        )
        
        exec_client.cache.add_order(limit_order)
        modify_order = TestCommandStubs.modify_order_command(
            order=limit_order,
            price=Price.from_int(2),
            quantity=Quantity.from_int(1),
        )
        
        exec_client._client.place_order = Mock()
        
        # Act
        await exec_client._modify_order(modify_order)
        
        # Assert
        sent_order = exec_client._client.place_order.call_args_list[0][0][0]
        assert sent_order.orderId == IBTestIdStubs.orderId()
        assert sent_order.orderRef == TestIdStubs.client_order_id().value
        assert sent_order.orderType == "LMT"
        assert sent_order.totalQuantity == Decimal("1")
        assert sent_order.action == "BUY"
        assert sent_order.tif == "GTC"
        assert sent_order.contract.conId == IBTestIdStubs.conId()
        assert sent_order.contract.exchange == "CME"
        assert sent_order.lmtPrice == 2.0
    
    @pytest.mark.asyncio()
    async def test_modify_order_response(
        self,
        exec_client,
    ):
        # Arrange
        instrument = TestInstrumentProvider.default_fx_ccy("AUD/USD", venue=Venue("SIM"))
        exec_client.instrument_provider.add(instrument)
        limit_order = TestExecStubs.limit_order()
        exec_client.cache.add_order(limit_order)
        exec_client.generate_order_updated = Mock()
        
        # Act
        event = IBTestExecutionStubs.open_order_event(
            status="Filled",
            totalQuantity=Decimal("2"),
            lmtPrice=Decimal("1"),
            orderId=400,
            orderType="LMT",
        )
        
        exec_client.open_order_callback(event)
        
        # Assert
        exec_client.generate_order_updated.assert_called_once()
        updated_kwargs = exec_client.generate_order_updated.call_args_list[0][1]
        assert updated_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert updated_kwargs["instrument_id"] == TestIdStubs.audusd_id()
        assert updated_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert updated_kwargs["venue_order_id"] == VenueOrderId("400")
        assert updated_kwargs["quantity"] == Decimal("2")
        assert updated_kwargs["price"] == Decimal("1")
        assert updated_kwargs["venue_order_id_modified"] == True
        
    @pytest.mark.asyncio()
    async def test_cancel_order_response(
        self,
        exec_client,
    ):
        # Arrange
        limit_order = TestExecStubs.limit_order()
        venue_order_id = VenueOrderId(str(IBTestIdStubs.orderId()))
        order_accepted = TestEventStubs.order_accepted(
            order=limit_order,
            venue_order_id=venue_order_id,
        )
        limit_order.apply(order_accepted)
        
        exec_client.cache.add_order(limit_order)
        exec_client.cache.update_order(limit_order)
        exec_client.generate_order_canceled = Mock()
        
        # Act
        error_event = IBErrorEvent(
            reqId=IBTestIdStubs.orderId(),
            errorCode=202,
            errorString="Order Canceled - reason",
            advancedOrderRejectJson="",
        )
        exec_client.error_callback(error_event)
        
        # Assert
        exec_client.generate_order_canceled.assert_called_once()
        cancel_kwargs = exec_client.generate_order_canceled.call_args_list[0][1]
        assert cancel_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert cancel_kwargs["instrument_id"] == TestIdStubs.audusd_id()
        assert cancel_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert cancel_kwargs["venue_order_id"] == venue_order_id
    
    @pytest.mark.asyncio()
    async def test_modify_order_rejected_response(
        self,
        exec_client,
    ):
        # Arrange
        limit_order = TestExecStubs.limit_order()
        exec_client.cache.add_order(limit_order)
        
        venue_order_id = VenueOrderId(str(IBTestIdStubs.orderId()))
        
        # accept
        order_accepted = TestEventStubs.order_accepted(
            order=limit_order,
            venue_order_id=venue_order_id,
        )
        limit_order.apply(order_accepted)
        exec_client.cache.update_order(limit_order)
        assert limit_order.venue_order_id == venue_order_id
        
        # modify
        
        order_pending_update = TestEventStubs.order_pending_update(
            order=limit_order,
        )
        limit_order.apply(order_pending_update)
        exec_client.cache.update_order(limit_order)
        
        exec_client.generate_order_modify_rejected = Mock()
        
        # Act
        error_event = IBErrorEvent(
            reqId=IBTestIdStubs.orderId(),
            errorCode=201,
            errorString="Order rejected - reason:YOUR ORDER IS NOT ACCEPTED",
            advancedOrderRejectJson="",
        )
        exec_client.error_callback(error_event)
        
        # Assert
        exec_client.generate_order_modify_rejected.assert_called_once()
        cancel_kwargs = exec_client.generate_order_modify_rejected.call_args_list[0][1]
        assert cancel_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert cancel_kwargs["instrument_id"] == TestIdStubs.audusd_id()
        assert cancel_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert cancel_kwargs["venue_order_id"] == venue_order_id
        assert cancel_kwargs["reason"] == error_event.errorString
    
    @pytest.mark.asyncio()
    async def test_order_rejected_response(
        self,
        exec_client,
    ):
        limit_order = TestExecStubs.limit_order()
        exec_client.cache.add_order(limit_order)
        
        venue_order_id = VenueOrderId(str(IBTestIdStubs.orderId()))
        
        # accept
        order_accepted = TestEventStubs.order_accepted(
            order=limit_order,
            venue_order_id=venue_order_id,
        )
        limit_order.apply(order_accepted)
        exec_client.cache.update_order(limit_order)
        assert limit_order.venue_order_id == venue_order_id
        
        exec_client.generate_order_rejected = Mock()
        
        # Act
        error_event = IBErrorEvent(
            reqId=IBTestIdStubs.orderId(),
            errorCode=201,
            errorString="Order rejected - reason:YOUR ORDER IS NOT ACCEPTED",
            advancedOrderRejectJson="",
        )
        exec_client.error_callback(error_event)
        
        # Assert
        exec_client.generate_order_rejected.assert_called_once()
        cancel_kwargs = exec_client.generate_order_rejected.call_args_list[0][1]
        assert cancel_kwargs["strategy_id"] == TestIdStubs.strategy_id()
        assert cancel_kwargs["instrument_id"] == TestIdStubs.audusd_id()
        assert cancel_kwargs["client_order_id"] == TestIdStubs.client_order_id()
        assert cancel_kwargs["reason"] == error_event.errorString
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_generate_position_status_reports(self, exec_client):
        reports = await exec_client.generate_position_status_reports()
        print(reports)
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_generate_position_status_report(self, exec_client):
        reports = await exec_client.generate_position_status_report()
        print(reports)

    @pytest.mark.skip(reason="TODO")
    async def test_generate_order_status_reports(self, exec_client):
        await self.client.connect()
        reports = await exec_client.generate_order_status_reports()
        print(reports)

    
    # async def _wait_for_order_status(self, order, status):
    #     print(f"Waiting for status {order_status_to_str(status)} for order {order.client_order_id}")
    #     while order.status != status:
    #         await asyncio.sleep(0)
        
    # @pytest.mark.skip(reason="not needed, fractional price reject at client level")
    # @pytest.mark.asyncio()
    # async def test_market_order_rejected_invalid_quantity(
    #     self,
    #     client,
    #     exec_client,
    #     cache,
    # ):
    #     instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

    #     market_order = TestExecStubs.market_order(
    #                 instrument_id=instrument_id,
    #                 order_side=OrderSide.BUY,
    #                 quantity=Quantity.from_int(1),
    #                 client_order_id=ClientOrderId("15"),
    #                 trader_id=TestIdStubs.trader_id(),
    #                 strategy_id=TestIdStubs.strategy_id(),
    #     )
    #     cache.add_order(market_order)

    #     submit_order = SubmitOrder(
    #                     trader_id=market_order.trader_id,
    #                     strategy_id=market_order.strategy_id,
    #                     order=market_order,
    #                     command_id=UUID4(),
    #                     ts_init=0,
    #     )

    #     message = b"4\x002\x0015\x0010318\x00This order doesn't support fractional quantity trading\x00\x00"
    #     def send_messages(_):
    #         client._handle_msg(message)

    #     send_mock = Mock(side_effect=send_messages)
    #     client._conn.sendMsg = send_mock

    #     await exec_client._submit_order(submit_order)

    #     await self._wait_for_order_status(market_order, OrderStatus.REJECTED)

    #     cached_order = cache.order(market_order.client_order_id)

    #     assert cached_order is not None
    #     assert cached_order.status == OrderStatus.REJECTED


# def setup(self):
#     clock = LiveClock()
#     logger = Logger(clock, level_stdout=LogLevel.INFO)

#     msgbus = MessageBus(
#         trader_id=TestIdStubs.trader_id(),
#         clock=clock,
#         logger=logger,
#     )
#     self.cache = TestComponentStubs.cache()

#     self.client = InteractiveBrokersClient(
#                     loop=asyncio.get_event_loop(),
#                     msgbus=msgbus,
#                     cache=self.cache,
#                     clock=clock,
#                     logger=logger,
#                     host="127.0.0.1",
#                     port=4002,
#                     client_id=1,

#     )
#     self.instrument_provider = InteractiveBrokersInstrumentProvider(client=self.client, logger=logger)
#     self.exec_client = InteractiveBrokersExecutionClient(
#         loop=asyncio.get_event_loop(),
#         client=self.client,
#         account_id=AccountId("InteractiveBrokers-"),
#         msgbus=msgbus,
#         cache=self.cache,
#         clock=clock,
#         logger=logger,
#         instrument_provider=self.instrument_provider,
#         ibg_client_id=1,
#     )

# @pytest.mark.asyncio()
    # async def test_limit_order_accepted(
    #     self,
    #     client,
    #     exec_client,
    #     cache,
    #     instrument_provider,
    # ):
    #     """
    #     messages = [
    #         b"5\x0029\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.16\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0029\x001\x002138440195\x000\x000\x000\x00\x002138440195.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0088.16\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
    #         b"3\x0029\x00Submitted\x000\x001\x000\x002138440195\x000\x000\x001\x00\x000\x00",
    #     ]
    #     """

    #     def send_messages(_):
    #         while len(messages) > 0:
    #             client._handle_msg(messages.pop(0))

    #     send_mock = Mock(side_effect=send_messages)
    #     client._conn.sendMsg = send_mock

    #     instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

    #     limit_order = TestExecStubs.limit_order(
    #         instrument_id=instrument_id,
    #         order_side=OrderSide.BUY,
    #         quantity=Quantity.from_int(1),
    #         client_order_id=ClientOrderId("29"),
    #         trader_id=TestIdStubs.trader_id(),
    #         strategy_id=TestIdStubs.strategy_id(),
    #         price=Price.from_str("87.16"),
    #     )

    #     cache.add_order(limit_order)

    #     submit_order = SubmitOrder(
    #         trader_id=limit_order.trader_id,
    #         strategy_id=limit_order.strategy_id,
    #         order=limit_order,
    #         command_id=UUID4(),
    #         ts_init=0,
    #     )

    #     await exec_client._submit_order(submit_order)

    #     await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

    #     cached_order = cache.order(limit_order.client_order_id)

    #     assert cached_order is not None
    #     assert cached_order.status == OrderStatus.ACCEPTED

    #     expected = b"\x00\x00\x01W3\x0029\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x0087.16\x00\x00GTC\x00\x00\x00\x000\x0029\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00"
    #     send_mock.assert_called_once_with(expected)
    #     # actual = send_mock.call_args_list[0][0][0]
    
# @pytest.mark.skip(reason="TODO")
    # async def test_limit_order_filled(
    #     self,
    #     client,
    #     order_setup,
    #     cache,
    #     instrument_provider,
    # ):
    

    #     def send_messages(_):
    #         while len(messages) > 0:
    #             client._handle_msg(messages.pop(0))

    #     send_mock = Mock(side_effect=send_messages)
    #     client._conn.sendMsg = send_mock

    #     instrument_id = InstrumentId.from_str("R[Z23].ICEEU")
    #     instrument = instrument_provider.find(instrument_id)

    #     market_order = await order_setup.submit_market_order(
    #         order_side=OrderSide.BUY,
    #         instrument_id=instrument_id,
    #         quantity=Quantity.from_str(str(instrument.info["minSize"])),
    #     )

    #     await asyncio.wait_for(
    #         self._wait_for_order_status(market_order, OrderStatus.FILLED),
    #         4,
    #     )

    #     cached_order = cache.order(market_order.client_order_id)

    #     assert cached_order is not None
    #     assert cached_order.status == OrderStatus.FILLED
    

    # @pytest.mark.skip(reason="TODO")
    # @pytest.mark.asyncio()
    # async def test_limit_order_filled(
    #     self,
    #     client,
    #     exec_client,
    #     cache,
    # ):
    #     messages = [
    #         b"3\x0032\x00PreSubmitted\x000\x001\x000\x002138440198\x000\x000\x001\x00\x000\x00",
    #         b"11\x00-1\x0032\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555acb5.01.01\x0020231116-16:47:50\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.78\x002138440198\x001\x000\x001\x0096.78\x0032\x00\x00\x00\x001\x00",
    #         b"5\x0032\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0096.84\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0032\x001\x002138440198\x000\x000\x000\x00\x002138440198.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.84\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
    #         b"3\x0032\x00Filled\x001\x000\x0096.78\x002138440198\x000\x0096.78\x001\x00\x000\x00",
    #         b"5\x0032\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0096.84\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0032\x001\x002138440198\x000\x000\x000\x00\x002138440198.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.84\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
    #         b"3\x0032\x00Filled\x001\x000\x0096.78\x002138440198\x000\x0096.78\x001\x00\x000\x00",
    #         b"59\x001\x000000e9b5.6555acb5.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
    #     ]

    #     def send_messages(_):
    #         while len(messages) > 0:
    #             client._handle_msg(messages.pop(0))

    #     send_mock = Mock(side_effect=send_messages)
    #     client._conn.sendMsg = send_mock

    #     instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

    #     limit_order = TestExecStubs.limit_order(
    #         instrument_id=instrument_id,
    #         order_side=OrderSide.BUY,
    #         quantity=Quantity.from_int(1),
    #         client_order_id=ClientOrderId("32"),
    #         trader_id=TestIdStubs.trader_id(),
    #         strategy_id=TestIdStubs.strategy_id(),
    #         price=Price.from_str("96.84"),
    #     )

    #     cache.add_order(limit_order)

    #     submit_order = SubmitOrder(
    #         trader_id=limit_order.trader_id,
    #         strategy_id=limit_order.strategy_id,
    #         order=limit_order,
    #         command_id=UUID4(),
    #         ts_init=0,
    #     )

    #     await exec_client._submit_order(submit_order)

    #     await self._wait_for_order_status(limit_order, OrderStatus.FILLED)

    #     cached_order = cache.order(limit_order.client_order_id)

    #     assert cached_order.status == OrderStatus.FILLED

    #     expected = b"\x00\x00\x01W3\x0032\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x0096.84\x00\x00GTC\x00\x00\x00\x000\x0032\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00"
    #     send_mock.assert_called_once_with(expected)
    
    # @pytest.mark.skip(reason="TODO")
    # @pytest.mark.asyncio()
    # async def test_limit_order_modify_price_and_quantity(
    #     self,
    #     client,
    #     exec_client,
    #     cache,
    # ):
    #     message_list = [
    #         [
    #             b"5\x0069\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.68\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x00311900000\x000\x000\x000\x00\x00311900000.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0088.68\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
    #             b"3\x0069\x00Submitted\x000\x001\x000\x00311900000\x000\x000\x001\x00\x000\x00",
    #         ],
    #         [
    #             b"5\x0069\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x002\x00LMT\x0088.68\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x00311900000\x000\x000\x000\x00\x00311900000.1/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
    #             b"3\x0069\x00Submitted\x000\x002\x000\x00311900000\x000\x000\x001\x00\x000\x00",
    #         ],
    #     ]

    #     def send_messages(_):
    #         messages = message_list.pop(0)
    #         for message in messages:
    #             client._handle_msg(message)

    #     send_mock = Mock(side_effect=send_messages)
    #     client._conn.sendMsg = send_mock

    #     instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

    #     limit_order = TestExecStubs.limit_order(
    #         instrument_id=instrument_id,
    #         order_side=OrderSide.BUY,
    #         quantity=Quantity.from_int(1),
    #         client_order_id=ClientOrderId("69"),
    #         trader_id=TestIdStubs.trader_id(),
    #         strategy_id=TestIdStubs.strategy_id(),
    #         price=Price.from_str("87.68"),
    #     )

    #     cache.add_order(limit_order)

    #     submit_order = SubmitOrder(
    #         trader_id=limit_order.trader_id,
    #         strategy_id=limit_order.strategy_id,
    #         order=limit_order,
    #         command_id=UUID4(),
    #         ts_init=0,
    #     )

    #     print(limit_order.client_order_id)
    #     print(limit_order.price)

    #     await exec_client._submit_order(submit_order)

    #     await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

    #     modify_order = ModifyOrder(
    #         trader_id=limit_order.trader_id,
    #         strategy_id=limit_order.strategy_id,
    #         instrument_id=limit_order.instrument_id,
    #         client_order_id=limit_order.client_order_id,
    #         venue_order_id=limit_order.venue_order_id,
    #         quantity=Quantity.from_int(2),
    #         price=Price.from_str("88.68"),
    #         trigger_price=None,
    #         command_id=UUID4(),
    #         ts_init=0,
    #     )

    #     await exec_client._modify_order(modify_order)

    #     await asyncio.sleep(0.00001)

    #     cached_order = cache.order(limit_order.client_order_id)

    #     assert cached_order.price == Price.from_str("88.68")
    #     assert cached_order.quantity == Quantity.from_int(2)

        # send_mock.assert_called_with(
        #     b"\x00\x00\x01W3\x0069\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x002.0\x00LMT\x0088.68\x00\x00GTC\x00\x00\x00\x000\x00\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        # )