from decimal import Decimal
import pandas as pd

from pyfutures.client.objects import IBOrderStatusEvent
from pyfutures.client.objects import IBOpenOrderEvent
from pyfutures.client.objects import IBExecutionEvent
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from ibapi.contract import Contract as IBContract
from ibapi.execution import Execution as IBExecution
from ibapi.commission_report import CommissionReport as IBCommissionReport
from pyfutures.tests.unit.adapter.stubs.identifiers import IBTestIdStubs

class IBTestExecutionStubs:
    
    @staticmethod
    def order_status_event() -> IBOrderStatusEvent:
        return IBOrderStatusEvent(
            orderId=IBTestIdStubs.orderId(),
            status="Submitted",
            filled=Decimal("0"),
            remaining=Decimal("0"),
            avgFillPrice=0.0,
            permId=0,
            parentId=0,
            lastFillPrice=0.0,
            clientId=1,
            whyHeld="",
            mktCapPrice=0.0,
        )
        
    @staticmethod
    def open_order_event(
        status: str = "Submitted",
        totalQuantity: Decimal | None = None,
        lmtPrice: Decimal | None = None,
        orderId: int | None = None,
        orderType: str | None = None,
    ) -> IBOpenOrderEvent:
        
        contract = IBContract()
        contract.conId = IBTestIdStubs.conId()
        contract.exchange = "CME"
        
        order = IBOrder()
        order.orderRef = TestIdStubs.client_order_id().value
        order.lmtPrice = lmtPrice or Decimal("1.2345")
        order.action = "BUY"
        order.orderId = orderId or IBTestIdStubs.orderId()
        order.orderType = orderType or "MKT"
        order.tif = "GTC"
        order.totalQuantity = totalQuantity or Decimal("1")
        order.filledQuantity = Decimal("0")
            
        order_state = IBOrderState()
        order_state.status = status
        
        return IBOpenOrderEvent(
            contract=contract,
            order=order,
            orderState=order_state,
        )
        
    @staticmethod
    def execution_event() -> IBExecutionEvent:
        execution = IBExecution()
        execution.orderId = IBTestIdStubs.orderId()
        
        execution.orderRef = TestIdStubs.client_order_id().value
        execution.execId = IBTestIdStubs.execId()
        execution.side = "BOT"
        execution.shares = Decimal("1")
        execution.price = 1.2345
        execution.time = 1700069390
        
        report = IBCommissionReport()
        report.commission = 1.2
        report.currency = "GBP"
        
        return IBExecutionEvent(
            timestamp=pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC"),
            reqId=-1,
            contract=IBContract,
            execution=execution,
            commissionReport=report,
        )