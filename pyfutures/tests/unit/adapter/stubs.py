import pandas as pd
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.contract import Contract as IBContract
from ibapi.execution import Execution as IBExecution
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.client.objects import IBExecutionEvent, IBOpenOrderEvent, IBOrderStatusEvent
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.enums import InstrumentClass
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.objects import Price
from nautilus_trader.model.instruments.futures_contract import FuturesContract
        
from decimal import Decimal

class AdapterStubs:
    
    @staticmethod
    def conId() -> int:
        return 1

    @staticmethod
    def orderId() -> int:
        return 10

    @staticmethod
    def reqId() -> int:
        return -10

    @staticmethod
    def execId() -> str:
        return "0000e9b5.6555a859.01.01"

    @classmethod
    def order_status_event(cls) -> IBOrderStatusEvent:
        return IBOrderStatusEvent(
            orderId=cls.orderId(),
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

    @classmethod
    def open_order_event(
        cls,
        status: str = "Submitted",
        totalQuantity: Decimal | None = None,
        lmtPrice: Decimal | None = None,
        orderId: int | None = None,
        orderType: str | None = None,
    ) -> IBOpenOrderEvent:

        contract = IBContract()
        contract.conId = cls.conId()
        contract.exchange = "CME"

        order = IBOrder()
        order.orderRef = TestIdStubs.client_order_id().value
        order.lmtPrice = lmtPrice or Decimal("1.2345")
        order.action = "BUY"
        order.orderId = orderId or cls.orderId()
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

    @classmethod
    def execution_event(cls) -> IBExecutionEvent:
        execution = IBExecution()
        execution.orderId = cls.orderId()

        execution.orderRef = TestIdStubs.client_order_id().value
        execution.execId = cls.execId()
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
        
    
    @staticmethod
    def mes_contract() -> FuturesContract:
        return FuturesContract(
            instrument_id=InstrumentId.from_str("MES=MES=FUT=2023Z.CME"),
            raw_symbol=Symbol("MES"),
            asset_class=AssetClass.COMMODITY,
            currency=Currency.from_str("GBP"),
            price_precision=4,
            price_increment=Price.from_str("0.0001"),
            multiplier=Quantity.from_int(1),
            lot_size=Quantity.from_int(1),
            underlying="MES",
            activation_ns=0,
            expiration_ns=0,
            ts_event=0,
            ts_init=0,
            info=dict(
                contract=dict(
                    conId=1,
                    exchange="CME",
                ),
            ),
        )
        