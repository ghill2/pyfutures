import asyncio

from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.common import Environment
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import RoutingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.objects import Price
from nautilus_trader.config import LoggingConfig

#
from pytower.tests.stubs.strategies import BuyOnBarX

from pyfutures.adapter.config import (
    InteractiveBrokersDataClientConfig,
)
from pyfutures.adapter.config import (
    InteractiveBrokersExecClientConfig,
)
from pyfutures.adapter.config import (
    InteractiveBrokersInstrumentProviderConfig,
)
from pyfutures.adapter.factories import (
    InteractiveBrokersLiveDataClientFactory,
)
from pyfutures.adapter.factories import (
    InteractiveBrokersLiveExecClientFactory,
)


# init_logging(level_stdout=LogLevel.DEBUG)


def test_strategy_logging():
    # Arrange
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # monkeypatch.setenv("BINANCE_API_KEY", "SOME_API_KEY")
    # monkeypatch.setenv("BINANCE_API_SECRET", "SOME_API_SECRET")
    # row = IBTestProviderStubs.universe_rows()[0]
    bar_type = BarType.from_str("EUR.GBP=CASH.IDEALPRO-5-SECOND-BID-EXTERNAL")

    provider_config = InteractiveBrokersInstrumentProviderConfig(
        chain_filters={
            "FMEU": lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
        },
        parsing_overrides={
            "MIX": {
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        },
        load_ids=[bar_type.instrument_id.value],
    )

    config = TradingNodeConfig(
        # logging=LoggingConfig(bypass_logging=True),
        environment=Environment.LIVE,
        logging=LoggingConfig(
            log_level="DEBUG",
            log_directory="logdir",
            log_level_file="DEBUG",
            log_file_format="log",
            # log_component_levels={"Portfolio": "INFO"},
        ),
        data_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersDataClientConfig(
                instrument_provider=provider_config, routing=RoutingConfig(default=True)
            ),
        },
        exec_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersExecClientConfig(
                instrument_provider=provider_config, routing=RoutingConfig(default=True)
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
    node.add_data_client_factory(
        "INTERACTIVE_BROKERS", InteractiveBrokersLiveDataClientFactory
    )
    node.add_exec_client_factory(
        "INTERACTIVE_BROKERS", InteractiveBrokersLiveExecClientFactory
    )

    node.build()
    strategy = BuyOnBarX(
        index=1,
        bar_type=bar_type,
        order_side=OrderSide.BUY,
        quantity=1,
    )
    strategytwo = BuyOnBarX(
        index=1,
        bar_type=bar_type,
        order_side=OrderSide.BUY,
        quantity=1,
    )

    # exec_client_id = ClientId("IB")
    node.trader.add_strategy(strategy)
    node.trader.add_strategy(strategytwo)

    node.portfolio.set_specific_venue(IB_VENUE)

    node.run()
