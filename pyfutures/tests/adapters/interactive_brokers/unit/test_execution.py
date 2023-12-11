import asyncio
from unittest.mock import Mock

import pytest

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import order_status_to_str
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


class TestInteractiveBrokersExecution:
    @pytest.mark.asyncio()
    async def test_generate_position_status_reports(self):
        await self.client.connect()
        reports = await self.exec_client.generate_position_status_reports()
        print(reports)

    @pytest.mark.asyncio()
    async def test_generate_position_status_report(self):
        await self.client.connect()
        reports = await self.exec_client.generate_position_status_report()
        print(reports)

    @pytest.mark.asyncio()
    async def test_generate_order_status_reports(self):
        await self.client.connect()
        reports = await self.exec_client.generate_order_status_reports()
        print(reports)

    @pytest.mark.asyncio()
    async def test_market_order_submitted(
        self,
        client,
        exec_client,
        cache,
    ):
        messages = [
            b"5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x005\x00Submitted\x000\x001\x000\x002138440174\x000\x000\x001\x00\x000\x00",
        ]

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("5"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
        )
        cache.add_order(market_order)

        submit_order = SubmitOrder(
            trader_id=market_order.trader_id,
            strategy_id=market_order.strategy_id,
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        await exec_client._submit_order(submit_order)

        await asyncio.wait_for(
            self._wait_for_order_status(market_order, OrderStatus.ACCEPTED),
            2,
        )

        cached_order = cache.order(market_order.client_order_id)

        assert cached_order is not None
        assert cached_order.status == OrderStatus.ACCEPTED
        assert cached_order.venue_order_id == VenueOrderId("5")
        send_mock.assert_called_once_with(
            b"\x00\x00\x01P3\x005\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00MKT\x00\x00\x00GTC\x00\x00\x00\x000\x005\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        )

    async def test_market_order_filled(
        self,
        client,
        exec_client,
        cache,
    ):
        messages = [
            b"5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x005\x00Submitted\x000\x001\x000\x002138440174\x000\x000\x001\x00\x000\x00",
            b"11\x00-1\x005\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555a859.01.01\x0020231116-12:15:35\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.79\x002138440174\x001\x000\x001\x0096.79\x005\x00\x00\x00\x001\x00",
            b"5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x005\x00Filled\x001\x000\x0096.79\x002138440174\x000\x0096.79\x001\x00\x000\x00",
            # b'5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00',
            # b'3\x005\x00Filled\x001\x000\x0096.79\x002138440174\x000\x0096.79\x001\x00\x000\x00',
            b"59\x001\x000000e9b5.6555a859.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
        ]

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        market_order = TestExecStubs.market_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("5"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
        )
        cache.add_order(market_order)

        submit_order = SubmitOrder(
            trader_id=market_order.trader_id,
            strategy_id=market_order.strategy_id,
            order=market_order,
            command_id=UUID4(),
            ts_init=0,
        )

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        await exec_client._submit_order(submit_order)

        await asyncio.wait_for(
            self._wait_for_order_status(market_order, OrderStatus.FILLED),
            2,
        )

        cached_order = cache.order(market_order.client_order_id)

        assert cached_order is not None
        assert cached_order.status == OrderStatus.FILLED
        assert cached_order.venue_order_id == VenueOrderId("5")
        send_mock.assert_called_once_with(
            b"\x00\x00\x01P3\x005\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00MKT\x00\x00\x00GTC\x00\x00\x00\x000\x005\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        )

    @pytest.mark.asyncio()
    async def test_limit_order_filled(
        self,
        client,
        order_setup,
        cache,
        instrument_provider,
    ):
        messages = [
            b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00PreSubmitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x003\x00PreSubmitted\x000\x001\x000\x002138440172\x000\x000\x001\x00\x000\x00",
            b"11\x00-1\x003\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555a7e7.01.01\x0020231116-12:07:51\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.90\x002138440172\x001\x000\x001\x0096.90\x003\x00\x00\x00\x001\x00",
            b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x003\x00Filled\x001\x000\x0096.90\x002138440172\x000\x0096.90\x001\x00\x000\x00",
            b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x003\x00Filled\x001\x000\x0096.90\x002138440172\x000\x0096.90\x001\x00\x000\x00",
            b"59\x001\x000000e9b5.6555a7e7.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
        ]

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")
        instrument = instrument_provider.find(instrument_id)

        market_order = await order_setup.submit_market_order(
            order_side=OrderSide.BUY,
            instrument_id=instrument_id,
            quantity=Quantity.from_str(str(instrument.info["minSize"])),
        )

        await asyncio.wait_for(
            self._wait_for_order_status(market_order, OrderStatus.FILLED),
            4,
        )

        cached_order = cache.order(market_order.client_order_id)

        assert cached_order is not None
        assert cached_order.status == OrderStatus.FILLED

    @pytest.mark.asyncio()
    async def test_limit_order_accepted(
        self,
        client,
        exec_client,
        cache,
        instrument_provider,
    ):
        messages = [
            b"5\x0029\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.16\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0029\x001\x002138440195\x000\x000\x000\x00\x002138440195.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0088.16\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x0029\x00Submitted\x000\x001\x000\x002138440195\x000\x000\x001\x00\x000\x00",
        ]

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("29"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("87.16"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order is not None
        assert cached_order.status == OrderStatus.ACCEPTED

        expected = b"\x00\x00\x01W3\x0029\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x0087.16\x00\x00GTC\x00\x00\x00\x000\x0029\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00"
        send_mock.assert_called_once_with(expected)
        # actual = send_mock.call_args_list[0][0][0]

    @pytest.mark.asyncio()
    async def test_limit_order_filled(
        self,
        client,
        exec_client,
        cache,
    ):
        messages = [
            b"3\x0032\x00PreSubmitted\x000\x001\x000\x002138440198\x000\x000\x001\x00\x000\x00",
            b"11\x00-1\x0032\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555acb5.01.01\x0020231116-16:47:50\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.78\x002138440198\x001\x000\x001\x0096.78\x0032\x00\x00\x00\x001\x00",
            b"5\x0032\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0096.84\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0032\x001\x002138440198\x000\x000\x000\x00\x002138440198.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.84\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x0032\x00Filled\x001\x000\x0096.78\x002138440198\x000\x0096.78\x001\x00\x000\x00",
            b"5\x0032\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0096.84\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0032\x001\x002138440198\x000\x000\x000\x00\x002138440198.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.84\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x0032\x00Filled\x001\x000\x0096.78\x002138440198\x000\x0096.78\x001\x00\x000\x00",
            b"59\x001\x000000e9b5.6555acb5.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
        ]

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("32"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("96.84"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.FILLED)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.status == OrderStatus.FILLED

        expected = b"\x00\x00\x01W3\x0032\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x0096.84\x00\x00GTC\x00\x00\x00\x000\x0032\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00"
        send_mock.assert_called_once_with(expected)

    @pytest.mark.asyncio()
    async def test_limit_order_cancel(
        self,
        cache,
        exec_client,
        client,
    ):
        message_list = [
            [
                b"5\x0036\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0086.78\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0036\x001\x002138440202\x000\x000\x000\x00\x002138440202.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0087.78\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0036\x00Submitted\x000\x001\x000\x002138440202\x000\x000\x001\x00\x000\x00",
            ],
            [
                b"3\x0036\x00Cancelled\x000\x001\x000\x002138440202\x000\x000\x001\x00\x000\x00",
                b"4\x002\x0036\x00202\x00Order Canceled - reason:\x00\x00",
            ],
        ]

        def send_messages(_):
            messages = message_list.pop(0)
            for message in messages:
                client._handle_msg(message)

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("36"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("86.78"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        cancel_order = CancelOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._cancel_order(cancel_order)

        await self._wait_for_order_status(limit_order, OrderStatus.CANCELED)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.status == OrderStatus.CANCELED

        send_mock.assert_called_with(b"\x00\x00\x00\x084\x001\x0036\x00\x00")

    @pytest.mark.asyncio()
    async def test_limit_order_modify_quantity(
        self,
        client,
        exec_client,
        cache,
    ):
        message_list = [
            [
                b"5\x0059\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0086.95\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x002138440226\x000\x000\x000\x00\x002138440226.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0087.95\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0059\x00Submitted\x000\x001\x000\x002138440226\x000\x000\x001\x00\x000\x00",
            ],
            [
                b"5\x0059\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x002\x00LMT\x0086.95\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x002138440226\x000\x000\x000\x00\x002138440226.1/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0059\x00Submitted\x000\x002\x000\x002138440226\x000\x000\x001\x00\x000\x00",
            ],
        ]

        def send_messages(_):
            messages = message_list.pop(0)
            for message in messages:
                client._handle_msg(message)

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("59"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("86.95"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        print(limit_order.client_order_id)
        print(limit_order.price)

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=Quantity.from_int(int(limit_order.quantity + 1)),
            price=limit_order.price,
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._modify_order(modify_order)

        await asyncio.sleep(0.00001)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.quantity == Quantity.from_int(2)

        send_mock.assert_called_with(
            b"\x00\x00\x01W3\x0059\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x002.0\x00LMT\x0086.95\x00\x00GTC\x00\x00\x00\x000\x00\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        )

    @pytest.mark.asyncio()
    async def test_limit_order_modify_price(
        self,
        client,
        exec_client,
        cache,
    ):
        message_list = [
            [
                b"5\x0061\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0086.95\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x002138440228\x000\x000\x000\x00\x002138440228.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0087.95\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0061\x00Submitted\x000\x001\x000\x002138440228\x000\x000\x001\x00\x000\x00",
            ],
            [
                b"5\x0061\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.95\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x002138440228\x000\x000\x000\x00\x002138440228.1/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0061\x00Submitted\x000\x001\x000\x002138440228\x000\x000\x001\x00\x000\x00",
            ],
        ]

        def send_messages(_):
            messages = message_list.pop(0)
            for message in messages:
                client._handle_msg(message)

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("61"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("86.95"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        print(limit_order.client_order_id)
        print(limit_order.price)

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=limit_order.quantity,
            price=Price.from_str("87.95"),
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._modify_order(modify_order)

        await asyncio.sleep(0.00001)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.price == Price.from_str("87.95")

        send_mock.assert_called_with(
            b"\x00\x00\x01U3\x0061\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x0087.95\x00\x00GTC\x00\x00\x00\x000\x00\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        )

    @pytest.mark.asyncio()
    async def test_limit_order_modify_price_and_quantity(
        self,
        client,
        exec_client,
        cache,
    ):
        message_list = [
            [
                b"5\x0069\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.68\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x00311900000\x000\x000\x000\x00\x00311900000.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0088.68\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0069\x00Submitted\x000\x001\x000\x00311900000\x000\x000\x001\x00\x000\x00",
            ],
            [
                b"5\x0069\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x002\x00LMT\x0088.68\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x00\x001\x00311900000\x000\x000\x000\x00\x00311900000.1/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
                b"3\x0069\x00Submitted\x000\x002\x000\x00311900000\x000\x000\x001\x00\x000\x00",
            ],
        ]

        def send_messages(_):
            messages = message_list.pop(0)
            for message in messages:
                client._handle_msg(message)

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        instrument_id = InstrumentId.from_str("R[Z23].ICEEU")

        limit_order = TestExecStubs.limit_order(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            client_order_id=ClientOrderId("69"),
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            price=Price.from_str("87.68"),
        )

        cache.add_order(limit_order)

        submit_order = SubmitOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            order=limit_order,
            command_id=UUID4(),
            ts_init=0,
        )

        print(limit_order.client_order_id)
        print(limit_order.price)

        await exec_client._submit_order(submit_order)

        await self._wait_for_order_status(limit_order, OrderStatus.ACCEPTED)

        modify_order = ModifyOrder(
            trader_id=limit_order.trader_id,
            strategy_id=limit_order.strategy_id,
            instrument_id=limit_order.instrument_id,
            client_order_id=limit_order.client_order_id,
            venue_order_id=limit_order.venue_order_id,
            quantity=Quantity.from_int(2),
            price=Price.from_str("88.68"),
            trigger_price=None,
            command_id=UUID4(),
            ts_init=0,
        )

        await exec_client._modify_order(modify_order)

        await asyncio.sleep(0.00001)

        cached_order = cache.order(limit_order.client_order_id)

        assert cached_order.price == Price.from_str("88.68")
        assert cached_order.quantity == Quantity.from_int(2)

        send_mock.assert_called_with(
            b"\x00\x00\x01W3\x0069\x00623496135\x00\x00\x00\x000.0\x00\x00\x00ICEEU\x00\x00\x00\x00\x00\x00\x00BUY\x002.0\x00LMT\x0088.68\x00\x00GTC\x00\x00\x00\x000\x00\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x001.7976931348623157e+308\x000\x00\x00\x00\x001.7976931348623157e+308\x00\x00\x00\x00\x000\x000\x000\x00\x002147483647\x002147483647\x000\x00\x00\x00",
        )

    async def _wait_for_order_status(self, order, status):
        print(f"Waiting for status {order_status_to_str(status)} for order {order.client_order_id}")
        while order.status != status:
            await asyncio.sleep(0)

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


#     self._log = LoggerAdapter(type(self).__name__, logger)
