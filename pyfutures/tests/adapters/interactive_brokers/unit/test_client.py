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

from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.client import IBOpenOrderEvent
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBExecutionEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBPositionEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs


RESPONSES_FOLDER = Path(__file__).parent / "responses"


class TestInteractiveBrokersClient:

    @pytest.mark.asyncio()
    async def test_request_contract_details_returns_expected(self, client):
        messages = [
            b"10\x000\x00D\x00FUT\x0020231124-12:30:00\x000\x00\x00ICEEUSOFT\x00USD\x00RCX3\x00RC\x00RC\x00553444806\x001\x0010\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,OPENCLOSE,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00ICEEUSOFT\x001\x0027655507\x00Robusta Coffee\x00\x00202311\x00\x00\x00\x00GB-Eire\x0020231115:0900-20231115:1730;20231116:0900-20231116:1730;20231117:0900-20231117:1730;20231118:CLOSED;20231119:CLOSED;20231120:0900-20231120:1730\x0020231115:0900-20231115:1730;20231116:0900-20231116:1730;20231117:0900-20231117:1730;20231118:CLOSED;20231119:CLOSED;20231120:0900-20231120:1730\x00\x00\x000\x002147483647\x00D\x00IND\x0033\x0020231124\x00\x001\x001\x001\x00",
            b"52\x001\x000\x00",
        ]

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(
            side_effect=send_messages,
        )

        client._conn.sendMsg = send_mock

        results = await asyncio.wait_for(client.request_contract_details(contract), 3)

        assert isinstance(results, list)
        assert len(results) == 1
        assert type(results[0]) == IBContractDetails

        send_mock.assert_called_once_with(
            b"\x00\x00\x00,9\x008\x000\x00553444806\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x000\x00\x00\x00\x00",
        )
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_request_contract_details_raises_exception(self, client):
        message = b"4\x002\x000\x00321\x00Error validating request.-'bQ' : cause - Please enter a valid security type\x00\x00"

        contract = Contract()
        contract.secType = "invalid_secType"
        contract.symbol = "D"
        contract.exchange = "ICEEUSOFT"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )

        client._conn.sendMsg = send_mock

        with pytest.raises(ClientException) as e:
            await client.request_contract_details(contract)
            assert e.code == 321

    @pytest.mark.asyncio()
    async def test_request_account_summary(self, client):
        messages = [
            b"63\x001\x000\x00DU1234567\x00AccountType\x00INDIVIDUAL\x00\x00",
            b"63\x001\x000\x00DU1234567\x00Cushion\x000.452835\x00\x00",
            b"63\x001\x000\x00DU1234567\x00DayTradesRemaining\x00-1\x00\x00",
            b"63\x001\x000\x00DU1234567\x00LookAheadNextChange\x001700073900\x00\x00",
            b"63\x001\x000\x00DU1234567\x00AccruedCash\x00-16475.82\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00AvailableFunds\x00549199.02\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00BuyingPower\x002327966.81\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00EquityWithLoanValue\x001160657.83\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00ExcessLiquidity\x00582033.62\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00FullAvailableFunds\x00548302.62\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00FullExcessLiquidity\x00581218.71\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00FullInitMarginReq\x00711135.25\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00FullMaintMarginReq\x00678261.07\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00GrossPositionValue\x000.00\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00InitMarginReq\x00710238.85\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00LookAheadAvailableFunds\x00549199.02\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00LookAheadExcessLiquidity\x00582033.62\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00LookAheadInitMarginReq\x00710238.85\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00LookAheadMaintMarginReq\x00677446.17\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00MaintMarginReq\x00677446.17\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00NetLiquidation\x001285310.14\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00PreviousDayEquityWithLoanValue\x001208301.71\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00SMA\x001228550.96\x00GBP\x00",
            b"63\x001\x000\x00DU1234567\x00TotalCashValue\x001301785.97\x00GBP\x00",
            b"64\x001\x000\x00",
        ]
        # await client.connect()

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(
            side_effect=send_messages,
        )

        client._conn.sendMsg = send_mock

        summary = await client.request_account_summary()

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
            b"\x00\x00\x01\xe962\x001\x000\x00All\x00AccountType,NetLiquidation,TotalCashValue,SettledCash,AccruedCash,BuyingPower,EquityWithLoanValue,PreviousDayEquityWithLoanValue,GrossPositionValue,ReqTEquity,ReqTMargin,SMA,InitMarginReq,MaintMarginReq,AvailableFunds,ExcessLiquidity,Cushion,FullInitMarginReq,FullMaintMarginReq,FullAvailableFunds,FullExcessLiquidity,LookAheadNextChange,LookAheadInitMarginReq,LookAheadMaintMarginReq,LookAheadAvailableFunds,LookAheadExcessLiquidity,HighestSeverity,DayTradesRemaining,Leverage\x00",
        )
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_request_next_order_id(self, client):
        message = b"9\x001\x004\x00"

        send_mock = Mock(
            side_effect=lambda _: client._handle_msg(message),
        )

        client._conn.sendMsg = send_mock

        next_id = await client.request_next_order_id()

        assert next_id == 4
        send_mock.assert_called_once_with(b"\x00\x00\x00\x068\x001\x001\x00")
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_place_market_order(self, client):
        contract = Contract()
        contract.conId = 564400671
        contract.exchange = "ICEEUSOFT"

        order = Order()
        order.contract = contract

        # MARKET order
        order.orderId = 1
        order.orderRef = "26ab199c-0171-4e33-b47c-9243fe78415b"  # client_order_id
        order.orderType = "MKT"  # order_type
        order.totalQuantity = Decimal(1)  # quantity
        order.action = "BUY"  # side

        send_mock = Mock(
            # side_effect=lambda _: client._handle_msg(messages.pop(0)),
        )

        client._conn.sendMsg = send_mock

        client.place_order(order)

        send_mock.assert_called_once_with(
            b"\x00\x00\x00\xd63\x001\x00564400671\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00MKT\x00\x00\x00\x00\x00\x00\x000\x0026ab199c-0171-4e33-b47c-9243fe78415b\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x000\x000\x000\x00\x00\x00\x000\x00\x00\x00",
        )

    @pytest.mark.asyncio()
    async def test_limit_place_order(self, client):
        contract = Contract()
        contract.conId = 564400671
        contract.exchange = "ICEEUSOFT"

        order = Order()
        order.contract = contract

        # LIMIT order
        order.orderId = 1
        order.orderRef = "36ab199c-0171-4e33-b47c-9243fe78415b"  # client_order_id
        order.orderType = "LMT"  # order_type
        order.totalQuantity = Decimal(1)  # quantity
        order.action = "BUY"  # side
        order.lmtPrice = 2400.0  # price
        order.tif = "GTC"  # time in force

        send_mock = Mock(
            # side_effect=lambda _: client._handle_msg(messages.pop(0)),
        )

        client._conn.sendMsg = send_mock

        client.place_order(order)

        send_mock.assert_called_once_with(
            b"\x00\x00\x00\xdf3\x001\x00564400671\x00\x00\x00\x000.0\x00\x00\x00ICEEUSOFT\x00\x00\x00\x00\x00\x00\x00BUY\x001\x00LMT\x002400.0\x00\x00GTC\x00\x00\x00\x000\x0036ab199c-0171-4e33-b47c-9243fe78415b\x001\x000\x000\x000\x000\x000\x000\x000\x00\x000\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x000\x00\x00\x000\x000\x00\x000\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x000\x00\x00\x000\x00\x000\x000\x000\x000\x00\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x000\x000\x000\x00\x00\x00\x000\x00\x00\x00",
        )

    @pytest.mark.asyncio()
    async def test_request_open_orders(self, client):
        await client.connect()

        messages = [
            b"5\x003\x00564400671\x00D\x00FUT\x0020240125\x000\x00?\x0010\x00ICEEUSOFT\x00USD\x00RCF4\x00RC\x00BUY\x001\x00LMT\x002400.0\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0048909417-95cb-4668-a70f-7886c71fb5a9\x001\x001866218892\x000\x000\x000\x00\x001866218892.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x000\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x003\x00Submitted\x000\x001\x000\x001866218892\x000\x000\x001\x00\x000\x00",
            b"5\x004\x00564400671\x00D\x00FUT\x0020240125\x000\x00?\x0010\x00ICEEUSOFT\x00USD\x00RCF4\x00RC\x00BUY\x001\x00MKT\x000.0\x000.0\x00DAY\x00\x00DU1234567\x00\x000\x00119dddc5-3cad-4b32-8d80-cf361c00d397\x001\x001771764360\x000\x000\x000\x00\x001771764360.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00PreSubmitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00",
            b"3\x004\x00PreSubmitted\x000\x001\x000\x001771764360\x000\x000\x001\x00\x000\x00",
            b"53\x001\x00",
        ]

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        orders = await client.request_open_orders()

        assert len(orders) == 2
        assert orders == [
            IBOpenOrderEvent(
                conId=564400671,
                totalQuantity=Decimal("1"),
                # filledQuantity=Decimal('170141183460469231731687303715884105727'),
                status="Submitted",
                lmtPrice=2400.0,
                action="BUY",
                orderId=3,
                orderType="LMT",
                tif="GTC",
                orderRef="48909417-95cb-4668-a70f-7886c71fb5a9",
            ),
            IBOpenOrderEvent(
                conId=564400671,
                totalQuantity=Decimal("1"),
                # filledQuantity=Decimal('170141183460469231731687303715884105727'),
                status="PreSubmitted",
                lmtPrice=0.0,
                action="BUY",
                orderId=4,
                orderType="MKT",
                tif="DAY",
                orderRef="119dddc5-3cad-4b32-8d80-cf361c00d397",
            ),
        ]

        send_mock.assert_called_once_with(b"\x00\x00\x00\x045\x001\x00")
        assert len(client.requests) == 0

    @pytest.mark.asyncio()
    async def test_request_positions(self, client):
        messages = [
            b"61\x003\x00DU1234567\x00586139726\x00MES\x00FUT\x0020231215\x000.0\x00\x005\x00\x00USD\x00MESZ3\x00MES\x003\x0022166.03666665\x00",
            b"61\x003\x00DU1234567\x00296349625\x00QM\x00FUT\x0020231117\x000.0\x00\x00500\x00\x00USD\x00QMZ3\x00QM\x0016\x0039148.16375\x00",
            b"61\x003\x00DU1234567\x00564400671\x00D\x00FUT\x0020240125\x000.0\x00\x0010\x00\x00USD\x00RCF4\x00RC\x008\x0024446.85\x00",
            b"62\x001\x00",
        ]

        def send_messages(_):
            while len(messages) > 0:
                client._handle_msg(messages.pop(0))

        send_mock = Mock(side_effect=send_messages)
        client._conn.sendMsg = send_mock

        positions = await asyncio.wait_for(client.request_positions(), 2)
        assert positions == [
            IBPositionEvent(conId=586139726, quantity=Decimal("3")),
            IBPositionEvent(conId=296349625, quantity=Decimal("16")),
            IBPositionEvent(conId=564400671, quantity=Decimal("8")),
        ]

        send_mock.assert_called_once_with(b"\x00\x00\x00\x0561\x001\x00")
        assert len(client.requests) == 0

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