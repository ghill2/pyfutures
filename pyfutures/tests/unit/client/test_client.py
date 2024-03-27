# import asyncio
import asyncio
import dataclasses
from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pandas as pd
import pytest
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.common import HistoricalTickLast
from ibapi.common import TickAttribBidAsk
from ibapi.contract import Contract
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.execution import Execution as IBExecution
from ibapi.order import Order
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState

from pyfutures.client.client import IBOpenOrderEvent
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import Frequency
from pyfutures.client.enums import WhatToShow
from pyfutures.client.objects import ClientException
from pyfutures.client.objects import ClientSubscription
from pyfutures.client.objects import IBErrorEvent
from pyfutures.client.objects import IBExecutionEvent
from pyfutures.client.objects import IBOrderStatusEvent
from pyfutures.client.objects import IBPositionEvent
from pyfutures.tests.unit.client.stubs import ClientStubs


@pytest.mark.asyncio()
async def test_eclient_sends_to_client(self):
    self.client = ClientStubs.client()
    # Arrange
    self.client.sendMsg = Mock()

    # Act
    self.client._eclient.reqManagedAccts()

    # Assert
    self.client.sendMsg.assert_called_once_with(b"\x00\x00\x00\x0517\x001\x00")


@pytest.mark.asyncio()
async def test_request_account_summary(self):
    # Arrange
    def send_mocked_response(*args, **kwargs):
        self.client.accountSummary(-10, "DU1234567", "AccountType", "INDIVIDUAL", "GBP")
        self.client.accountSummary(-10, "DU1234567", "Cushion", "0.452835", "GBP")
        self.client.accountSummary(-10, "DU1234567", "DayTradesRemaining", "-1", "GBP")
        self.client.accountSummary(
            -10, "DU1234567", "LookAheadNextChange", "1700073900", "GBP"
        )
        self.client.accountSummary(-10, "DU1234567", "AccruedCash", "-16475.82", "GBP")
        self.client.accountSummary(
            -10, "DU1234567", "AvailableFunds", "549199.02", "GBP"
        )
        self.client.accountSummary(-10, "DU1234567", "BuyingPower", "2327966.81", "GBP")
        self.client.accountSummary(
            -10, "DU1234567", "EquityWithLoanValue", "1160657.83", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "ExcessLiquidity", "582033.62", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "FullAvailableFunds", "548302.62", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "FullExcessLiquidity", "581218.71", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "FullInitMarginReq", "711135.25", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "FullMaintMarginReq", "678261.07", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "GrossPositionValue", "0.00", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "InitMarginReq", "710238.85", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "LookAheadAvailableFunds", "549199.02", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "LookAheadExcessLiquidity", "582033.62", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "LookAheadInitMarginReq", "710238.85", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "LookAheadMaintMarginReq", "677446.17", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "MaintMarginReq", "677446.17", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "NetLiquidation", "1285310.14", "GBP"
        )
        self.client.accountSummary(
            -10, "DU1234567", "PreviousDayEquityWithLoanValue", "1208301.71", "GBP"
        )
        self.client.accountSummary(-10, "DU1234567", "SMA", "1228550.96", "GBP")
        self.client.accountSummary(
            -10, "DU1234567", "TotalCashValue", "1301785.97", "GBP"
        )
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


# @pytest.mark.asyncio()
# async def test_place_market_order(self):
#     # Arrange
#     order = Order()
#     order.orderId = 4
#     order.contract = Contract()
#
#     send_mock = Mock()
#     self.client._eclient.placeOrder = send_mock
#
#     # Act
#     self.client.place_order(order)
#
#     # Assert
#     send_mock.assert_called_once_with(
#         order.orderId,
#         order.contract,
#         order,
#     )


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_cancel_order(self):
    pass


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
def test_request_last_bar(self):
    pass


@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
def test_request_bars_filters_bars_outside_range(self):
    pass


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
