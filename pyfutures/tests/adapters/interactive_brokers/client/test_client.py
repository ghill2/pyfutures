import asyncio
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.contract import Contract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState

from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.client import IBOpenOrderEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBExecutionEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBPositionEvent
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs


RESPONSES_FOLDER = Path(__file__).parent / "responses"


class TestInteractiveBrokersClient:

    @pytest.mark.asyncio()
    async def test_request_contract_details_returns_expected(self, client):
        
        # Arrange
        contract = Contract()
        
        def send_mocked_responses(*args, **kwargs):
            client.contractDetails(-10, IBContractDetails())
            client.contractDetailsEnd(-10)
            
        send_mock = Mock(side_effect=send_mocked_responses)
        client._eclient.reqContractDetails = send_mock
        
        # Act
        results = await client.request_contract_details(contract)
        
        # Assert
        assert isinstance(results, list)
        assert all(isinstance(x, IBContractDetails) for x in results)
        assert send_mock.call_args_list[0][1] == {
            "reqId": -10,
            "contract": contract,
        }
        
    @pytest.mark.asyncio()
    async def test_request_contract_details_raises_exception(self, client):
        
        # Arrange
        contract = Contract()
        
        def send_mocked_response(*args, **kwargs):
            client.error(-10, 321, "test")
        send_mock = Mock(side_effect=send_mocked_response)
        
        client._eclient.reqContractDetails = send_mock
        
        # Act & Assert
        with pytest.raises(ClientException) as e:
            await client.request_contract_details(contract)
            assert e.code == 321

    @pytest.mark.asyncio()
    async def test_request_account_summary(self, client):
        
        # Arrange
        def send_mocked_responses(*args, **kwargs):
            client.accountSummary(-10, "DU1234567", "AccountType", "INDIVIDUAL", "GBP")
            client.accountSummary(-10, "DU1234567", "Cushion", "0.452835", "GBP")
            client.accountSummary(-10, "DU1234567", "DayTradesRemaining", "-1", "GBP")
            client.accountSummary(-10, "DU1234567", "LookAheadNextChange", "1700073900", "GBP")
            client.accountSummary(-10, "DU1234567", "AccruedCash", "-16475.82", "GBP")
            client.accountSummary(-10, "DU1234567", "AvailableFunds", "549199.02", "GBP")
            client.accountSummary(-10, "DU1234567", "BuyingPower", "2327966.81", "GBP")
            client.accountSummary(-10, "DU1234567", "EquityWithLoanValue", "1160657.83", "GBP")
            client.accountSummary(-10, "DU1234567", "ExcessLiquidity", "582033.62", "GBP")
            client.accountSummary(-10, "DU1234567", "FullAvailableFunds", "548302.62", "GBP")
            client.accountSummary(-10, "DU1234567", "FullExcessLiquidity", "581218.71", "GBP")
            client.accountSummary(-10, "DU1234567", "FullInitMarginReq", "711135.25", "GBP")
            client.accountSummary(-10, "DU1234567", "FullMaintMarginReq", "678261.07", "GBP")
            client.accountSummary(-10, "DU1234567", "GrossPositionValue", "0.00", "GBP")
            client.accountSummary(-10, "DU1234567", "InitMarginReq", "710238.85", "GBP")
            client.accountSummary(-10, "DU1234567", "LookAheadAvailableFunds", "549199.02", "GBP")
            client.accountSummary(-10, "DU1234567", "LookAheadExcessLiquidity", "582033.62", "GBP")
            client.accountSummary(-10, "DU1234567", "LookAheadInitMarginReq", "710238.85", "GBP")
            client.accountSummary(-10, "DU1234567", "LookAheadMaintMarginReq", "677446.17", "GBP")
            client.accountSummary(-10, "DU1234567", "MaintMarginReq", "677446.17", "GBP")
            client.accountSummary(-10, "DU1234567", "NetLiquidation", "1285310.14", "GBP")
            client.accountSummary(-10, "DU1234567", "PreviousDayEquityWithLoanValue", "1208301.71", "GBP")
            client.accountSummary(-10, "DU1234567", "SMA", "1228550.96", "GBP")
            client.accountSummary(-10, "DU1234567", "TotalCashValue", "1301785.97", "GBP")
            client.accountSummaryEnd(-10)
            
        send_mock = Mock(side_effect=send_mocked_responses)
        client._eclient.reqAccountSummary = send_mock
        
        # Act
        summary = await client.request_account_summary()
        
        # Assert
        assert isinstance(summary, dict)
        assert summary == {
            "AccountType": "INDIVIDUAL",
            "AccruedCash": "-16475.82",
            "AvailableFunds": "549199.02",
            "BuyingPower": "2327966.81",
            "Cushion": "0.452835",
            "DayTradesRemaining": "-1",
            "EquityWithLoanValue": "1160657.83",
            "ExcessLiquidity": "582033.62",
            "FullAvailableFunds": "548302.62",
            "FullExcessLiquidity": "581218.71",
            "FullInitMarginReq": "711135.25",
            "FullMaintMarginReq": "678261.07",
            "GrossPositionValue": "0.00",
            "InitMarginReq": "710238.85",
            "LookAheadAvailableFunds": "549199.02",
            "LookAheadExcessLiquidity": "582033.62",
            "LookAheadInitMarginReq": "710238.85",
            "LookAheadMaintMarginReq": "677446.17",
            "LookAheadNextChange": "1700073900",
            "MaintMarginReq": "677446.17",
            "NetLiquidation": "1285310.14",
            "PreviousDayEquityWithLoanValue": "1208301.71",
            "SMA": "1228550.96",
            "TotalCashValue": "1301785.97",
            "account": "DU1234567",
            "currency": "GBP",
        }
        send_mock.assert_called_once_with(
            reqId=-10,
            groupName="All",
            tags=AccountSummaryTags.AllTags,
        )

    @pytest.mark.asyncio()
    async def test_request_next_order_id(self, client):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            client.nextValidId(4)
        send_mock = Mock(side_effect=send_mocked_response)
        client._eclient.reqIds = send_mock
        
        # Act
        next_id = await client.request_next_order_id()

        assert next_id == 4
        send_mock.assert_called_once_with(1)

    @pytest.mark.asyncio()
    async def test_place_market_order(self, client):
        
        # Arrange
        order = Order()
        order.orderId = 4
        order.contract = Contract()
        
        send_mock = Mock()
        client._eclient.placeOrder = send_mock
        
        # Act
        client.place_order(order)
        
        # Assert
        send_mock.assert_called_once_with(
            order.orderId,
            order.contract,
            order,
        )

    @pytest.mark.asyncio()
    async def test_request_open_orders(self, client):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
            client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
            client.openOrderEnd()
            
        send_mock = Mock(side_effect=send_mocked_response)
        client._eclient.reqOpenOrders = send_mock
        
        # Act
        orders = await client.request_open_orders()
        
        # Assert
        assert len(orders) == 2
        assert all(isinstance(o, IBOpenOrderEvent) for o in orders)
        send_mock.assert_called_once()

    @pytest.mark.asyncio()
    async def test_request_positions(self, client):

        # Arrange
        def send_mocked_responses(*args, **kwargs):
            client.position("DU1234567", IBContract(), Decimal("1"), 1.0)
            client.position("DU1234567", IBContract(), Decimal("1"), 1.0)
            client.positionEnd()

        send_mock = Mock(side_effect=send_mocked_responses)
        client._eclient.reqPositions = send_mock
        
        # Act
        positions = await client.request_positions()
        
        # Assert
        assert len(positions) == 2
        assert all(isinstance(p, IBPositionEvent) for p in positions)
        send_mock.assert_called_once()
        
    @pytest.mark.asyncio()
    async def test_request_executions(self, client):
        messages = [
            b"11\x000\x001\x00564400671\x00D\x00FUT\x0020240125\x000.0\x00\x0010\x00ICEEUSOFT\x00USD\x00RCF4\x00RC\x000001b269.6554510d.01.01\x0020231115 09:20:44 GB-Eire\x00DU1234567\x00ICEEUSOFT\x00BOT\x001\x002498.00\x001866218890\x001\x000\x001\x002498.00\x00d7eb38a3-4f8c-4073-8fa8-1ba2c9c61ffe\x00\x00\x00\x001\x00",
            b"11\x000\x002\x00564400671\x00D\x00FUT\x0020240125\x000.0\x00\x0010\x00ICEEUSOFT\x00USD\x00RCF4\x00RC\x000001b269.6554510e.01.01\x0020231115 09:20:44 GB-Eire\x00DU1234567\x00ICEEUSOFT\x00BOT\x001\x002498.00\x001866218891\x001\x000\x001\x002498.00\x000d13b34c-c80f-4335-b704-cb226f445691\x00\x00\x00\x001\x00",
            b"59\x001\x000001b269.6554510d.01.01\x003.1\x00USD\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
            b"59\x001\x000001b269.6554510e.01.01\x003.1\x00USD\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00",
            b"55\x001\x000\x00",
        ]
        # await client.connect()

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        executions = await asyncio.wait_for(client.request_executions(), 3)

        assert executions == [
            IBExecutionEvent(
                reqId=0,
                conId=564400671,
                orderId=1,
                execId="0001b269.6554510d.01.01",
                side="BOT",
                shares=Decimal("1"),
                price=2498.0,
                commission=3.1,
                commissionCurrency="USD",
                time="20231115 09:20:44 GB-Eire",
            ),
            IBExecutionEvent(
                reqId=0,
                conId=564400671,
                orderId=2,
                execId="0001b269.6554510e.01.01",
                side="BOT",
                shares=Decimal("1"),
                price=2498.0,
                commission=3.1,
                commissionCurrency="USD",
                time="20231115 09:20:44 GB-Eire",
            ),
        ]
        send_mock.assert_called_once_with(
            b"\x00\x00\x00\x0e7\x003\x000\x000\x00\x00\x00\x00\x00\x00\x00",
        )
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_request_head_timestamp_single(self, client):
        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        message = b"88\x000\x0020220329-08:00:00\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock
        timestamp = await client.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID,
        )

        assert str(timestamp) == "2022-03-29 08:00:00+00:00"
        send_mock.assert_called_once_with(
            b"\x00\x00\x00087\x000\x00553444806\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x000\x001\x00BID\x001\x00",
        )
        assert len(client.requests) == 0

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

        message = b"97\x000\x003\x001700069390\x000\x002720.00\x002735.00\x001\x001\x001700069392\x000\x002720.00\x002736.00\x001\x001\x001700069395\x000\x002721.00\x002736.00\x001\x001\x001\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock

        quotes = await client.request_quote_ticks(
            name="test",
            contract=contract,
            count=2,
        )

        assert len(quotes) == 3
        assert all(isinstance(quote, IBQuoteTick) for quote in quotes)
        assert len(client.requests) == 0
        # send_mock.assert_called_once_with(
        #     b'\x00\x00\x00N96\x000\x00553444806\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x000\x00\x0020231115 21:22:38 UTC\x002\x00BID_ASK\x001\x000\x00\x00'
        # )

    @pytest.mark.asyncio()
    async def test_request_trade_ticks(self, client):
        contract = Contract()
        contract.conId = 564400671
        contract.exchange = "ICEEUSOFT"

        message = b"98\x000\x003\x001700069395\x000\x002539.00\x001\x00\x00\x001700069395\x000\x002541.00\x001\x00\x00\x001700069395\x000\x002541.00\x001\x00\x00\x001\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock

        trades = await client.request_trade_ticks(
            name="test",
            contract=contract,
            count=2,
        )

        assert len(trades) == 3
        assert all(isinstance(trade, IBTradeTick) for trade in trades)
        assert len(client.requests) == 0

    @pytest.mark.skip(reason="market closed")
    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks(self, client):
        await client.connect()

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

        await asyncio.wait_for(wait_for_quote_tick(), 10)

        assert callback_mock.call_count > 0

    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks_cancel(self, client):
        pass

    @pytest.mark.asyncio()
    async def test_request_bars_daily(self, client):
        # await client.connect()

        message = b"17\x000\x0020231111-21:34:26\x0020231115-21:34:26\x004\x0020231110\x002603.00\x002616.00\x002577.00\x002596.00\x00-1\x00-1\x00-1\x0020231113\x002587.00\x002687.00\x002574.00\x002681.00\x00-1\x00-1\x00-1\x0020231114\x002673.00\x002697.00\x002637.00\x002653.00\x00-1\x00-1\x00-1\x0020231115\x002639.00\x002732.00\x002639.00\x002721.00\x00-1\x00-1\x00-1\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        bars = await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_DAY,
            duration=Duration(4, Frequency.DAY),
            what_to_show=WhatToShow.BID,
        )
        assert bars == [
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-10 00:00:00+0000", tz="UTC"),
                open=2603.0,
                high=2616.0,
                low=2577.0,
                close=2596.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-13 00:00:00+0000", tz="UTC"),
                open=2587.0,
                high=2687.0,
                low=2574.0,
                close=2681.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-14 00:00:00+0000", tz="UTC"),
                open=2673.0,
                high=2697.0,
                low=2637.0,
                close=2653.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-15 00:00:00+0000", tz="UTC"),
                open=2639.0,
                high=2732.0,
                low=2639.0,
                close=2721.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
        ]

        assert len(bars) == 4
        assert all(isinstance(bar, IBBar) for bar in bars)
        send_mock.assert_called_once_with(
            b"\x00\x00\x00>20\x000\x00553444806\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x000\x00\x001 day\x004 D\x001\x00BID\x002\x000\x00\x00",
        )
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_request_bars_minute(self, client):
        # await client.connect()

        message = b"17\x000\x0020231115-22:14:33\x0020231115-22:17:33\x003\x001700069220\x002729.00\x002729.00\x002726.00\x002726.00\x00-1\x00-1\x00-1\x001700069280\x002726.00\x002726.00\x002723.00\x002723.00\x00-1\x00-1\x00-1\x001700069340\x002723.00\x002723.00\x002720.00\x002721.00\x00-1\x00-1\x00-1\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        bars = await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_MINUTE,
            duration=Duration(60 * 3, Frequency.SECOND),
            what_to_show=WhatToShow.BID,
        )

        assert bars == [
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-15 17:27:00+0000", tz="UTC"),
                open=2729.0,
                high=2729.0,
                low=2726.0,
                close=2726.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-15 17:28:00+0000", tz="UTC"),
                open=2726.0,
                high=2726.0,
                low=2723.0,
                close=2723.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
            IBBar(
                name=None,
                time=pd.Timestamp("2023-11-15 17:29:00+0000", tz="UTC"),
                open=2723.0,
                high=2723.0,
                low=2720.0,
                close=2721.0,
                volume=Decimal("-1"),
                wap=Decimal("-1"),
                count=-1,
            ),
        ]
        assert len(bars) == 3
        assert all(isinstance(bar, IBBar) for bar in bars)
        send_mock.assert_called_once_with(
            b"\x00\x00\x00@20\x000\x00553444806\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x000\x00\x001 min\x00180 S\x001\x00BID\x002\x000\x00\x00",
        )
        assert len(client.requests) == 0

    @pytest.mark.skip(reason="market closed")
    @pytest.mark.asyncio()
    async def test_subscribe_bars_realtime(self, client):
        callback_mock = AsyncMock()

        client.bar_events += callback_mock

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_bars(
            name="test",
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
        )

        async def wait_for_bar():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_bar(), 2)

        assert callback_mock.call_count > 0

    @pytest.mark.skip(reason="market closed")
    @pytest.mark.asyncio()
    async def test_subscribe_bars_historical(self, client):
        callback_mock = AsyncMock()

        client.bar_events += callback_mock

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_bars(
            name="test",
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._15_SECOND,
        )

        async def wait_for_bar():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_bar(), 2)

        assert callback_mock.call_count > 0

    @pytest.mark.asyncio()
    async def test_subscribe_bars_cancel(self, client):
        pass

    @pytest.mark.asyncio()
    async def test_subscribe_order_status_events(self, client):
        pass

    @pytest.mark.asyncio()
    async def test_subscribe_execution_events(self, client):
        pass

    @pytest.mark.asyncio()
    async def test_subscribe_error_events(self, client):
        pass

    @pytest.mark.asyncio()
    async def test_request_accounts(self, client):
        message = b"15\x001\x00DU1234567\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )
        client._conn.sendMsg = send_mock

        accounts = await client.request_accounts()
        assert accounts == ["DU1234567"]

        send_mock.assert_called_once_with(b"\x00\x00\x00\x0517\x001\x00")
