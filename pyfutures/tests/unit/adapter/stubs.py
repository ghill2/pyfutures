from decimal import Decimal

import pandas as pd
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.contract import Contract as IBContract
from ibapi.execution import Execution as IBExecution
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.objects import IBExecutionEvent
from pyfutures.client.objects import IBOpenOrderEvent
from pyfutures.client.objects import IBOrderStatusEvent

PROVIDER_CONFIG = dict(
    chain_filters={
        "FMEU": lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
    },
    parsing_overrides={
        "MIX": {
            "price_precision": 0,
            "price_increment": Price(5, 0),
        },
    },
)


class AdapterStubs:
    @staticmethod
    def provider_config():
        return PROVIDER_CONFIG

    @staticmethod
    def instrument_provider(client: InteractiveBrokersClient) -> InteractiveBrokersInstrumentProvider:
        config = InteractiveBrokersInstrumentProviderConfig(**PROVIDER_CONFIG)
        provider = InteractiveBrokersInstrumentProvider(client=client, config=config)
        return provider

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
    def contract(
        instrument_id: InstrumentId | None = None,
    ) -> FuturesContract:
        instrument_id = instrument_id or InstrumentId.from_str("MES=MES=FUT=2023Z.CME")
        return FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=instrument_id.symbol,
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
    
    @staticmethod
    def continuous_bar(self):
        return ContinuousBar(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_bar=Bar(
                bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("1.1"),
                high=Price.from_str("1.3"),
                low=Price.from_str("1.0"),
                close=Price.from_str("1.2"),
                volume=Quantity.from_int(1),
                ts_init=0,
                ts_event=0,
            ),
            forward_bar=Bar(
                bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("2.1"),
                high=Price.from_str("2.3"),
                low=Price.from_str("2.0"),
                close=Price.from_str("2.2"),
                volume=Quantity.from_int(2),
                ts_init=0,
                ts_event=0,
            ),
            previous_bar=Bar(
                bar_type=BarType.from_str("MES=2021V.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("3.1"),
                high=Price.from_str("3.3"),
                low=Price.from_str("3.0"),
                close=Price.from_str("3.2"),
                volume=Quantity.from_int(3),
                ts_init=0,
                ts_event=0,
            ),
            carry_bar=Bar(
                bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("4.1"),
                high=Price.from_str("4.3"),
                low=Price.from_str("4.0"),
                close=Price.from_str("4.2"),
                volume=Quantity.from_int(4),
                ts_init=0,
                ts_event=0,
            ),
            ts_event=0,
            ts_init=0,
        )