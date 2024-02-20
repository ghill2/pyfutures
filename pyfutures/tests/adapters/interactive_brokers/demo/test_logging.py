from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
import pytest


from pyfutures.adapters.interactive_brokers.factories import InteractiveBrokersLiveDataClientFactory
from pyfutures.adapters.interactive_brokers.factories import InteractiveBrokersLiveExecClientFactory
from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.adapters.interactive_brokers.config import IBMarketDataTypeEnum
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersInstrumentProviderConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.examples.strategies.subscribe import SubscribeStrategy
from nautilus_trader.examples.strategies.subscribe import SubscribeStrategyConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.data import BarType
from nautilus_trader.common import Environment
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import Price

#
from pytower.tests.stubs.strategies import BuyOnBarX
import asyncio

def test_strategy_logging():
    # Arrange
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)


    # monkeypatch.setenv("BINANCE_API_KEY", "SOME_API_KEY")
    # monkeypatch.setenv("BINANCE_API_SECRET", "SOME_API_SECRET")
    #
    row = IBTestProviderStubs.universe_rows()[0]
    bar_type = BarType.from_str(f"{row.instrument_id}-1-DAY-BID-EXTERNAL")

    provider_config = InteractiveBrokersInstrumentProviderConfig(
        chain_filters={
            'FMEU': lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
        },
        parsing_overrides={
            "MIX": {
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        },
        load_ids=[bar_type.instrument_id.value]
    )

    config = TradingNodeConfig(
        # logging=LoggingConfig(bypass_logging=True),
        environment=Environment.LIVE,
        data_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersDataClientConfig(
                instrument_provider=provider_config
            ),
        },
        exec_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersExecClientConfig(
                instrument_provider=provider_config
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
    strategy = BuyOnBarX(
        index=1,
        bar_type=bar_type,
        order_side=OrderSide.BUY,
        quantity=1,
    )
    # add instrument to the cache,



    node.add_data_client_factory("INTERACTIVE_BROKERS", InteractiveBrokersLiveDataClientFactory)
    node.add_exec_client_factory("INTERACTIVE_BROKERS", InteractiveBrokersLiveExecClientFactory)

    node.build()

    # provider = node._exec_engine._clients[0].instrument_provider()
    exec_client_id = ClientId("IB")
    provider = node.trader._exec_engine._clients[exec_client_id]._instrument_provider
    provider.load_contract(row.contract_cont)

    node.trader.add_strategy(strategy)


    node.portfolio.set_specific_venue(IB_VENUE)


    # Act
    node.run()
    # asyncio.sleep(2.0)
