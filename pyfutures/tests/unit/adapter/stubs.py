from decimal import Decimal
import asyncio

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
from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.common import Environment
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import RoutingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Price
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.continuous.bar import ContinuousBar
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.factories import InteractiveBrokersLiveDataClientFactory
from pyfutures.adapter.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.tests.unit.adapter.stubs import AdapterStubs as UnitAdapterStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.data import ContinuousData
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
    
    @classmethod
    def continuous_data(cls, reconciliation: bool = False) -> ContinuousData:
        return ContinuousData(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            strategy_id=TestIdStubs.strategy_id(),
            config=RollConfig(
                hold_cycle=RollCycle("HMUZ"),
                priced_cycle=RollCycle("FGHJKMNQUVXZ"),
                roll_offset=-5,
                approximate_expiry_offset=14,
                carry_offset=1,
            ),
            reconciliation=reconciliation,
            instrument_provider=cls.instrument_provider(),
        )
        
    @staticmethod
    def trading_node(
        trader_id: TraderId | None = None,
        load_ids: list | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        
        trader_id = trader_id or TestIdStubs.trader_id()
        loop = loop or asyncio.get_event_loop()
        
        provider_config_dict = dict(
            load_ids=load_ids,
            **UnitAdapterStubs.provider_config(),
        )
        provider_config = InteractiveBrokersInstrumentProviderConfig(**provider_config_dict)

        config = TradingNodeConfig(
            trader_id=trader_id,
            environment=Environment.LIVE,
            data_clients={
                "INTERACTIVE_BROKERS": InteractiveBrokersDataClientConfig(
                    instrument_provider=provider_config,
                    routing=RoutingConfig(default=True),
                ),
            },
            exec_clients={
                "INTERACTIVE_BROKERS": InteractiveBrokersExecClientConfig(
                    instrument_provider=provider_config,
                    routing=RoutingConfig(default=True),
                ),
            },
            timeout_disconnection=1.0,  # Short timeout for testing
            timeout_post_stop=1.0,  # Short timeout for testing
            exec_engine=LiveExecEngineConfig(
                reconciliation=False,
                inflight_check_interval_ms=0,
                debug=True,
            ),
        )
        node = TradingNode(config=config, loop=loop)

        # add instrument to the cache,
        node.add_data_client_factory("INTERACTIVE_BROKERS", InteractiveBrokersLiveDataClientFactory)
        node.add_exec_client_factory("INTERACTIVE_BROKERS", InteractiveBrokersLiveExecClientFactory)

        node.build()

        node.portfolio.set_specific_venue(IB_VENUE)

        return node