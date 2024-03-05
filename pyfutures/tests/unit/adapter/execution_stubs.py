from decimal import Decimal

from pyfutures.adapter.client.objects import IBOrderStatusEvent
from pyfutures.adapter.client.objects import IBOpenOrderEvent
from pyfutures.adapter.client.objects import IBExecutionEvent
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from ibapi.contract import Contract as IBContract
from ibapi.execution import Execution as IBExecution
from ibapi.commission_report import CommissionReport as IBCommissionReport

class IBTestExecutionStubs:
    
    @staticmethod
    def order_status_event() -> IBOrderStatusEvent:
        return IBOrderStatusEvent(
            orderId=5,
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
    def open_order_event() -> IBOpenOrderEvent:
        
        contract = IBContract()
        contract.conId = 1
        contract.exchange = "CME"
        
        order = IBOrder()
        order.orderRef = TestIdStubs.client_order_id().value
        order.lmtPrice = Decimal("1.2345")
        order.action = "BUY"
        order.orderId = 5
        order.orderType = "MKT"
        order.tif = "GTC"
        order.totalQuantity=Decimal("1")
        order.filledQuantity=Decimal("0")
            
        order_state = IBOrderState()
        order_state.status = "Submitted"
        
        return IBOpenOrderEvent(
            contract=contract,
            order=order,
            orderState=order_state,
        )
        
    @staticmethod
    def execution_event(
        orderRef: str,
    ) -> IBExecutionEvent:
        execution = IBExecution()
        execution.orderId = 5
        execution.orderRef = orderRef
        execution.execId = "0000e9b5.6555a859.01.01"
        execution.side = "BOT"
        execution.shares = Decimal("1")
        execution.price = 1.2345
        execution.time = 1700069390
        
        report = IBCommissionReport()
        report.commission = 1.2
        report.currency = "GBP"
        
        return IBExecutionEvent(
            reqId=-1,
            contract=IBContract,
            execution=execution,
            commissionReport=report,
        )