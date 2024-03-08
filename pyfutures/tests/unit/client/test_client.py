# import asyncio
import time
import asyncio
from decimal import Decimal
from pathlib import Path
import dataclasses
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.contract import Contract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.common import HistoricalTickLast
from ibapi.order import Order
from ibapi.common import BarData
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from ibapi.common import TickAttribBidAsk
from ibapi.order_state import OrderState as IBOrderState
from pyfutures.client.objects import ClientSubscription
from ibapi.execution import Execution as IBExecution
from ibapi.commission_report import CommissionReport as IBCommissionReport
from pyfutures.client.objects import ClientException
from pyfutures.client.client import IBOpenOrderEvent
from pyfutures.client.objects import IBExecutionEvent
from pyfutures.client.objects import IBOrderStatusEvent
from pyfutures.client.objects import IBErrorEvent
from pyfutures.client.objects import IBExecutionEvent
from pyfutures.client.objects import IBPositionEvent
from pyfutures.client.objects import IBPortfolioEvent
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import Duration
from pyfutures.adapter.enums import Frequency
from pyfutures.adapter.enums import WhatToShow
from ibapi.common import HistoricalTickBidAsk
from pyfutures.tests.unit.client.stubs import ClientStubs

pytestmark = pytest.mark.unit

class TestInteractiveBrokersClient:
    
    def setup_method(self):
        self.client = ClientStubs.client()
    
    @pytest.mark.asyncio()
    async def test_eclient_sends_to_client(self):
        
        # Arrange
        self.client.sendMsg = Mock()
        
        # Act
        self.client._eclient.reqManagedAccts()
        
        # Assert
        self.client.sendMsg.assert_called_once_with(b'\x00\x00\x00\x0517\x001\x00')
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_reset(self):
        pass
        
    
    @pytest.mark.skip(reason="hangs when running entire test suite")
    @pytest.mark.asyncio()
    async def test_queue_processes_messages(self):
        
        # Arrange
        mock_connection = Mock()
        mock_connection.connect = AsyncMock()
        mock_connection.is_connected = lambda _: True
        mock_connection.sendMsg = Mock()
        self.client._connection = mock_connection
        await self.client.connect()
        
        # Act
        self.client.sendMsg(b"message1")
        self.client.sendMsg(b"message2")
        await asyncio.sleep(0)
        
        # Assert
        calls = mock_connection.sendMsg.call_args_list
        assert calls[0][0][0] == b"message1"
        assert calls[1][0][0] == b"message2"
            
    @pytest.mark.asyncio()
    async def test_request_contract_details_returns_expected(self):
        
        # Arrange
        contract = Contract()
        
        def send_mocked_response(*args, **kwargs):
            self.client.contractDetails(-10, IBContractDetails())
            self.client.contractDetailsEnd(-10)
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqContractDetails = send_mock
        
        # Act
        results = await self.client.request_contract_details(contract)
        
        # Assert
        assert isinstance(results, list)
        assert all(isinstance(x, IBContractDetails) for x in results)
        assert send_mock.call_args_list[0][1] == {
            "reqId": -10,
            "contract": contract,
        }
        
    @pytest.mark.asyncio()
    async def test_request_contract_details_raises_exception(self):
        
        # Arrange
        contract = Contract()
        
        def send_mocked_response(*args, **kwargs):
            self.client.error(-10, 321, "test")
        send_mock = Mock(side_effect=send_mocked_response)
        
        self.client._eclient.reqContractDetails = send_mock
        
        # Act & Assert
        with pytest.raises(ClientException) as e:
            await self.client.request_contract_details(contract)
            assert e.code == 321
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_last_contract_month(self) -> str:
        pass
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_front_contract_details(self):
        pass
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_front_contract(self):
        pass
    
    @pytest.mark.asyncio()
    async def test_request_account_summary(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.accountSummary(-10, "DU1234567", "AccountType", "INDIVIDUAL", "GBP")
            self.client.accountSummary(-10, "DU1234567", "Cushion", "0.452835", "GBP")
            self.client.accountSummary(-10, "DU1234567", "DayTradesRemaining", "-1", "GBP")
            self.client.accountSummary(-10, "DU1234567", "LookAheadNextChange", "1700073900", "GBP")
            self.client.accountSummary(-10, "DU1234567", "AccruedCash", "-16475.82", "GBP")
            self.client.accountSummary(-10, "DU1234567", "AvailableFunds", "549199.02", "GBP")
            self.client.accountSummary(-10, "DU1234567", "BuyingPower", "2327966.81", "GBP")
            self.client.accountSummary(-10, "DU1234567", "EquityWithLoanValue", "1160657.83", "GBP")
            self.client.accountSummary(-10, "DU1234567", "ExcessLiquidity", "582033.62", "GBP")
            self.client.accountSummary(-10, "DU1234567", "FullAvailableFunds", "548302.62", "GBP")
            self.client.accountSummary(-10, "DU1234567", "FullExcessLiquidity", "581218.71", "GBP")
            self.client.accountSummary(-10, "DU1234567", "FullInitMarginReq", "711135.25", "GBP")
            self.client.accountSummary(-10, "DU1234567", "FullMaintMarginReq", "678261.07", "GBP")
            self.client.accountSummary(-10, "DU1234567", "GrossPositionValue", "0.00", "GBP")
            self.client.accountSummary(-10, "DU1234567", "InitMarginReq", "710238.85", "GBP")
            self.client.accountSummary(-10, "DU1234567", "LookAheadAvailableFunds", "549199.02", "GBP")
            self.client.accountSummary(-10, "DU1234567", "LookAheadExcessLiquidity", "582033.62", "GBP")
            self.client.accountSummary(-10, "DU1234567", "LookAheadInitMarginReq", "710238.85", "GBP")
            self.client.accountSummary(-10, "DU1234567", "LookAheadMaintMarginReq", "677446.17", "GBP")
            self.client.accountSummary(-10, "DU1234567", "MaintMarginReq", "677446.17", "GBP")
            self.client.accountSummary(-10, "DU1234567", "NetLiquidation", "1285310.14", "GBP")
            self.client.accountSummary(-10, "DU1234567", "PreviousDayEquityWithLoanValue", "1208301.71", "GBP")
            self.client.accountSummary(-10, "DU1234567", "SMA", "1228550.96", "GBP")
            self.client.accountSummary(-10, "DU1234567", "TotalCashValue", "1301785.97", "GBP")
            self.client.accountSummaryEnd(-10)
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqAccountSummary = send_mock
        
        # Act
        summary = await self.client.request_account_summary()
        
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
    async def test_request_next_order_id(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.nextValidId(4)
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqIds = send_mock
        
        # Act
        next_id = await self.client.request_next_order_id()

        assert next_id == 4
        send_mock.assert_called_once_with(1)

    @pytest.mark.asyncio()
    async def test_place_market_order(self):
        
        # Arrange
        order = Order()
        order.orderId = 4
        order.contract = Contract()
        
        send_mock = Mock()
        self.client._eclient.placeOrder = send_mock
        
        # Act
        self.client.place_order(order)
        
        # Assert
        send_mock.assert_called_once_with(
            order.orderId,
            order.contract,
            order,
        )
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_cancel_order(self):
        pass
        
    @pytest.mark.asyncio()
    async def test_request_open_orders(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
            self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
            self.client.openOrderEnd()
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqOpenOrders = send_mock
        
        # Act
        orders = await self.client.request_open_orders()
        
        # Assert
        assert len(orders) == 2
        assert all(isinstance(o, IBOpenOrderEvent) for o in orders)
        send_mock.assert_called_once()
    
    @pytest.mark.asyncio()
    async def test_request_open_orders_no_event_emit(self):
        
        """
        when there is an active open orders request an event should not be emitted
        """
        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
            self.client.orderStatus(
                orderId=4,
                status="FILLED",
                filled=Decimal("1"),
                remaining=Decimal("1"),
                avgFillPrice=1.23,
                permId=5,
                parentId=6,
                lastFillPrice=1.43,
                clientId=1,
                whyHeld="reason",
                mktCapPrice=1.76,
            )
            self.client.openOrderEnd()
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqOpenOrders = send_mock
        
        open_order_callback_mock = AsyncMock()
        self.client.open_order_events += open_order_callback_mock
        
        order_status_callback_mock = AsyncMock()
        self.client.order_status_events += order_status_callback_mock
        
        # Act
        await self.client.request_open_orders()
        
        # Assert
        order_status_callback_mock.assert_not_called()
        open_order_callback_mock.assert_not_called()
        
        
    @pytest.mark.asyncio()
    async def test_request_positions(self):

        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.position("DU1234567", IBContract(), Decimal("1"), 1.0)
            self.client.position("DU1234567", IBContract(), Decimal("1"), 1.0)
            self.client.positionEnd()

        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqPositions = send_mock
        
        # Act
        positions = await self.client.request_positions()
        
        # Assert
        assert len(positions) == 2
        assert all(isinstance(p, IBPositionEvent) for p in positions)
        send_mock.assert_called_once()
        
    @pytest.mark.asyncio()
    async def test_request_executions_returns_expected(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            execution = IBExecution()
            execution.execId = 1
            execution.time = "20231116-12:07:51"
            
            report = IBCommissionReport()
            report.execId = execution.execId
            self.client.execDetails(-10, IBContract(), execution)
            self.client.commissionReport(report)
            
            execution = IBExecution()
            execution.execId = 2
            execution.time = "20231116-12:07:51"
            
            report = IBCommissionReport()
            report.execId = execution.execId
            self.client.execDetails(-10, IBContract(), execution)
            self.client.commissionReport(report)
            
            self.client.execDetailsEnd(-10)

        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqExecutions = send_mock
        
        # Act
        executions = await self.client.request_executions(client_id=1)
        
        # Assert
        assert len(executions) == 2
        assert all(isinstance(e, IBExecutionEvent) for e in executions)
        send_mock.assert_called_once()
    
    @pytest.mark.asyncio()
    async def test_request_executions_no_event_emit(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            execution = IBExecution()
            execution.execId = 1
            execution.time = "20231116-12:07:51"
            
            report = IBCommissionReport()
            report.execId = execution.execId
            self.client.execDetails(-10, IBContract(), execution)
            self.client.commissionReport(report)
            
            execution = IBExecution()
            execution.execId = 2
            execution.time = "20231116-12:07:51"
            
            report = IBCommissionReport()
            report.execId = execution.execId
            self.client.execDetails(-10, IBContract(), execution)
            self.client.commissionReport(report)
            
            self.client.execDetailsEnd(-10)

        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqExecutions = send_mock
        
        callback_mock = AsyncMock()
        self.client.execution_events += callback_mock
        
        # Act
        await self.client.request_executions(client_id=1)
        
        # Assert
        callback_mock.assert_not_called()
        
    @pytest.mark.asyncio()
    async def test_request_head_timestamp(self):
        
        # Arrange
        contract = Contract()
        what_to_show = WhatToShow.BID
        
        def send_mocked_response(*args, **kwargs):
            self.client.headTimestamp(-10, "20220329-08:00:00")
        send_mock = Mock(side_effect=send_mocked_response)
        
        self.client._eclient.reqHeadTimeStamp = send_mock
        
        # Act
        timestamp = await self.client.request_head_timestamp(
            contract=contract,
            what_to_show=what_to_show,
        )
        
        # Assert
        assert timestamp == pd.Timestamp("2022-03-29 08:00:00+00:00", tz="UTC")
        assert send_mock.call_args_list[0][1] == dict(
            contract=contract,
            formatDate=1,
            reqId=-10,
            useRTH=True,
            whatToShow="BID",
        )
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_first_quote_tick(self):
        pass
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_last_quote_tick(self):
        pass
        
    @pytest.mark.asyncio()
    async def test_request_quote_ticks(self):
        
        # Act
        contract = Contract()
        
        tick = HistoricalTickBidAsk()
        tick.time = 1700069390
        start_time = pd.Timestamp("2023-01-01 08:00:00", tz="UTC")
        end_time = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        
        def send_mocked_response(*args, **kwargs):
            
            self.client.historicalTicksBidAsk(-10, [tick], False)
            self.client.historicalTicksBidAsk(-10, [tick], True)
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqHistoricalTicks = send_mock
        
        # Act
        quotes = await self.client.request_quote_ticks(
            contract=contract,
            start_time=start_time,
            end_time=end_time,
            count=2,
        )
        
        # Assert
        assert len(quotes) == 2
        assert all(isinstance(q, HistoricalTickBidAsk) for q in quotes)
        assert quotes[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        assert quotes[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        assert send_mock.call_args_list[0][1] == dict(
            reqId=-10,
            contract=contract,
            startDateTime="20230101-08:00:00",
            endDateTime="20230101-12:00:00",
            numberOfTicks=2,
            whatToShow="BID_ASK",
            useRth=True,
            ignoreSize=False,
            miscOptions=[],
        )

    @pytest.mark.asyncio()
    async def test_request_trade_ticks(self):
        
        # Arrange
        contract = Contract()
        
        tick = HistoricalTickLast()
        tick.time = 1700069390
        start_time = pd.Timestamp("2023-01-01 08:00:00", tz="UTC")
        end_time = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        
        def send_mocked_response(*args, **kwargs):
            self.client.historicalTicksLast(-10, [tick], False)
            self.client.historicalTicksLast(-10, [tick], True)
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqHistoricalTicks = send_mock
        
        # Act
        trades = await self.client.request_trade_ticks(
            contract=contract,
            start_time=start_time,
            end_time=end_time,
            count=2,
        )
        
        # Assert
        assert len(trades) == 2
        assert all(isinstance(q, HistoricalTickLast) for q in trades)
        assert trades[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        assert trades[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        assert send_mock.call_args_list[0][1] == dict(
            reqId=-10,
            contract=contract,
            startDateTime="20230101-08:00:00",
            endDateTime="20230101-12:00:00",
            numberOfTicks=2,
            whatToShow="TRADES",
            useRth=True,
            ignoreSize=False,
            miscOptions=[],
        )

    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks_sends_expected(self):
        
        # Arrange
        contract = Contract()
        send_mock = Mock()
        self.client._eclient.reqTickByTickData = send_mock
        
        # Act
        subscription = self.client.subscribe_quote_ticks(
            contract=contract,
            callback=Mock(),
        )
        assert isinstance(subscription, ClientSubscription)
        
        # Assert
        sent_kwargs = send_mock.call_args_list[0][1]
        assert sent_kwargs == dict(
            reqId=-10,
            contract=contract,
            tickType="BidAsk",
            numberOfTicks=0,
            ignoreSize=True,
        )
        
    
    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks_returns_expected(self):
        
        tickAttribBidAsk = TickAttribBidAsk()
        
        def send_mocked_response(*args, **kwargs):
            self.client.tickByTickBidAsk(
                reqId=-10,
                time=1700069390,
                bidPrice=1.1,
                askPrice=1.2,
                bidSize=Decimal("1"),
                askSize=Decimal("1"),
                tickAttribBidAsk=tickAttribBidAsk,
            )
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqTickByTickData = send_mock
        
        callback_mock = Mock()
        
        # Act
        self.client.subscribe_quote_ticks(
            contract=Contract(),
            callback=callback_mock,
        )
        
        # Assert
        expected = dict(
            timestamp=pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC"),
            time=1700069390,
            priceBid=1.1,
            priceAsk=1.2,
            sizeBid=Decimal("1"),
            sizeAsk=Decimal("1"),
            tickAttribBidAsk=tickAttribBidAsk,
        )
        tick_response = callback_mock.call_args_list[0][0][0]
        assert tick_response.__dict__ == expected
        
    @pytest.mark.asyncio()
    async def test_unsubscribe_quote_ticks(self):
        
        # Arrange
        self.client._eclient.reqTickByTickData = Mock()
        
        cancel_mock = Mock()
        self.client._eclient.cancelTickByTickData = cancel_mock
        
        # Act
        subscription = self.client.subscribe_quote_ticks(
            contract=Contract(),
            callback=Mock(),
        )
        subscription.cancel()
        
        # Assert
        cancel_mock.assert_called_once_with(reqId=-10)
        assert subscription not in self.client.subscriptions
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    def test_request_last_bar(self):
        pass
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    def test_request_bars_filters_bars_outside_range(self):
        pass
    
    @pytest.mark.skip(reason="broken after re-connect changes")
    @pytest.mark.asyncio()
    async def test_request_bars_daily(self):

        # Arrange
        contract = Contract()
        
        end_time = pd.Timestamp("2023-11-16", tz="UTC")
        
        def send_mocked_response(*args, **kwargs):
            bar1 = BarData()
            bar1.date = 1700069390  # TODO: find correct timestamp format
            bar2 = BarData()
            bar2.date = 1700069390  # TODO: find correct timestamp format
            self.client.historicalData(-10, bar2)
            self.client.historicalData(-10, bar1)
            self.client.historicalDataEnd(-10, "", "")
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqHistoricalData = send_mock
        
        # Act
        bars = await client.request_bars(
            contract=contract,
            bar_size=BarSize._1_DAY,
            duration=Duration(4, Frequency.DAY),
            what_to_show=WhatToShow.BID,
            end_time=end_time,
        )
        
        # Assert
        assert len(bars) == 2
        assert isinstance(bars, list)
        assert all(isinstance(bar, BarData) for bar in bars)
        assert bars[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        assert bars[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        
        sent_kwargs = send_mock.call_args_list[0][1]
        assert sent_kwargs == dict(
            reqId=-10,
            contract=contract,
            endDateTime="20231116-00:00:00",
            durationStr="4 D",
            barSizeSetting="1 day",
            whatToShow="BID",
            useRTH=True,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )
    
    @pytest.mark.skip(reason="TODO")
    @pytest.mark.asyncio()
    async def test_request_bars_minute(self):
        pass

    @pytest.mark.asyncio()
    async def test_subscribe_bars_5_seconds_sends_expected(self):
        
        # Arrange
        contract = Contract()
        send_mock = Mock()
        self.client._eclient.reqRealTimeBars = send_mock
        
        # Act
        subscription = self.client.subscribe_bars(
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=Mock(),
        )
        assert isinstance(subscription, ClientSubscription)
        
        # Assert
        sent_kwargs = send_mock.call_args_list[0][1]
        assert sent_kwargs == dict(
            reqId=-10,
            contract=contract,
            barSize="",  # currently being ignored
            whatToShow="BID",
            useRTH=True,
            realTimeBarsOptions=[],
        )

    @pytest.mark.asyncio()
    async def test_subscribe_bars_5_seconds_returns_expected(self):
        
        def send_mocked_response(*args, **kwargs):
            self.client.realtimeBar(
                reqId=-10,
                time=1700069390,
                open_=1.1,
                high=1.1,
                low=1.1,
                close=1.1,
                volume=Decimal("1"),
                wap=Decimal("2"),
                count=1,
            )
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqRealTimeBars = send_mock
        
        callback_mock = Mock()
        
        # Act
        self.client.subscribe_bars(
            contract=Contract(),
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=callback_mock,
        )
        
        # Assert
        expected = dict(
            timestamp=pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC"),
            date=1700069390,
            open=1.1,
            high=1.1,
            low=1.1,
            close=1.1,
            volume=Decimal("1"),
            wap=Decimal("2"),
            barCount=1,
        )
        
        bar_response = callback_mock.call_args_list[0][0][0]
        assert bar_response.__dict__ == expected
        
    @pytest.mark.asyncio()
    async def test_unsubscribe_5_second_bars(self):
        
        # Arrange
        self.client._eclient.reqRealTimeBars = Mock()
        
        cancel_mock = Mock()
        self.client._eclient.cancelRealTimeBars = cancel_mock
        
        # Act
        subscription = self.client.subscribe_bars(
            contract=Contract(),
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=Mock(),
        )
        subscription.cancel()
        
        # Assert
        cancel_mock.assert_called_once_with(reqId=-10)
        assert subscription not in self.client.subscriptions
    
    @pytest.mark.asyncio()
    async def test_subscribe_bars_historical_sends_expected(self):
        
        # Arrange
        contract = Contract()
        send_mock = Mock()
        self.client._eclient.reqHistoricalData = send_mock
        
        # Act
        subscription = self.client.subscribe_bars(
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._1_MINUTE,
            callback=Mock(),
        )
        assert isinstance(subscription, ClientSubscription)
        
        # Assert
        sent_kwargs = send_mock.call_args_list[0][1]
        assert sent_kwargs == dict(
            reqId=-10,
            contract=contract,
            endDateTime="",
            durationStr="60 S",
            barSizeSetting="1 min",
            whatToShow="BID",
            useRTH=True,
            formatDate=1,
            keepUpToDate=True,
            chartOptions=[],
        )

    @pytest.mark.asyncio()
    async def test_subscribe_bars_historical_returns_expected(self):
        
        def send_mocked_response(*args, **kwargs):
            bar = BarData()
            bar.date = 1700069390
            self.client.historicalDataUpdate(-10, bar)
            
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqHistoricalData = send_mock
        
        callback_mock = Mock()
        
        # Act
        self.client.subscribe_bars(
            contract=Contract(),
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._1_MINUTE,
            callback=callback_mock,
        )
        
        # Assert
        bar = callback_mock.call_args_list[0][0][0]
        assert isinstance(bar, BarData)
        assert bar.timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
        
    @pytest.mark.asyncio()
    async def test_unsubscribe_historical_second_bars(self):
        
        # Arrange
        self.client._eclient.reqHistoricalData = Mock()
        
        cancel_mock = Mock()
        self.client._eclient.cancelHistoricalData = cancel_mock
        
        # Act
        subscription = self.client.subscribe_bars(
            contract=Contract(),
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._1_MINUTE,
            callback=Mock(),
        )
        subscription.cancel()
        
        # Assert
        cancel_mock.assert_called_once_with(reqId=-10)
        assert subscription not in self.client.subscriptions
    
    @pytest.mark.asyncio()
    async def test_request_accounts(self):
        
        # Arrange
        def send_mocked_response(*args, **kwargs):
            self.client.managedAccounts("DU1234567,DU1234568")
        
        send_mock = Mock(side_effect=send_mocked_response)
        self.client._eclient.reqManagedAccts = send_mock
        
        # Act
        accounts = await self.client.request_accounts()
        
        # Assert
        assert accounts == ["DU1234567", "DU1234568"]
        send_mock.assert_called_once()
            
    @pytest.mark.asyncio()
    async def test_subscribe_order_status_events(self):
        
        # Arrange
        expected = dict(
            orderId=4,
            status="FILLED",
            filled=Decimal("1"),
            remaining=Decimal("1"),
            avgFillPrice=1.2,
            permId=5,
            parentId=6,
            lastFillPrice=1.4,
            clientId=9,
            whyHeld="test",
            mktCapPrice=1.32,
        )
        callback_mock = AsyncMock()
        self.client.order_status_events += callback_mock
        
        # Act
        self.client.orderStatus(**expected)
        
        # Assert
        event_response = callback_mock.call_args_list[0][0][0]
        assert isinstance(event_response, IBOrderStatusEvent)
        assert dataclasses.asdict(event_response) == expected
    
    @pytest.mark.asyncio()
    async def test_subscribe_execution_events(self):
        
        """
        INFO:InteractiveBrokersClient:openOrder 7, orderStatus PreSubmitted, commission: 1.7976931348623157e+308, completedStatus: 
        INFO:InteractiveBrokersClient:execDetails reqId=-1 ExecId: 0000e1a7.65e1df89.01.01, Time: 20240301-18:17:09, Account: DU7779554, Exchange: CME, Side: BOT, Shares: 1, Price: 16.81, PermId: 1432478520, ClientId: 1, OrderId: 7, Liquidation: 0, CumQty: 1, AvgPrice: 16.81, OrderRef: aa3ab6d6-3ff8-4630-af41-b5a0e27f2f06, EvRule: , EvMultiplier: 0, ModelCode: , LastLiquidity: 1
        INFO:InteractiveBrokersClient:openOrder 7, orderStatus Filled, commission: 1.7976931348623157e+308, completedStatus: 
        INFO:InteractiveBrokersClient:openOrder 7, orderStatus Filled, commission: 2.97USD, completedStatus: 
        INFO:InteractiveBrokersClient:commissionReport
        """
        
        # Arrange
        callback_mock = AsyncMock()
        self.client.execution_events += callback_mock
        
        # Act
        execution = IBExecution()
        execution.execId = "0000e1a7.65e1df89.01.01"
        execution.time = "20231116-12:07:51"
        
        self.client.execDetails(-1, Contract(), execution)
        
        report = IBCommissionReport()
        report.execId = execution.execId
        self.client.commissionReport(report)
        
        # Assert
        event_response = callback_mock.call_args_list[0][0][0]
        assert isinstance(event_response, IBExecutionEvent)
        
    @pytest.mark.asyncio()
    async def test_subscribe_error_events(self):
        
        # Arrange
        callback_mock = AsyncMock()
        self.client.error_events += callback_mock
        
        # Act
        self.client.error(
            reqId=4,
            errorCode=5,
            errorString="error message",
            advancedOrderRejectJson="",
        )
        
        # Assert
        event_response = callback_mock.call_args_list[0][0][0]
        assert isinstance(event_response, IBErrorEvent)
        assert event_response.reqId == 4
        assert event_response.errorCode == 5
        assert event_response.errorString == "error message"
        assert event_response.advancedOrderRejectJson == ""
    
    @pytest.mark.asyncio()
    async def test_subscribe_account_updates(self):
        pass
    
    @pytest.mark.asyncio()
    async def test_request_historical_schedule(self):
        pass
    
    @pytest.mark.asyncio()
    async def test_request_portfolio(self):
        pass
        
        