import asyncio
import logging

# init_logging(level_stdout=LogLevel.DEBUG)
import sys

from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.common import Environment
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import RoutingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Price
from pytower.tests.stubs.strategies import BuyOnBarX

from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.factories import InteractiveBrokersLiveDataClientFactory
from pyfutures.adapter.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.logger import LoggerAttributes


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


# init_logging(level_stdout=LogLevel.DEBUG)


def create_trading_node(trader_id: TraderId):
    # Arrange
    loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)

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
        trader_id=trader_id,
        environment=Environment.LIVE,
        # logging=LoggingConfig(
        # log_level="debug",
        # log_directory="logdir",
        # log_level_file="DEBUG",
        # log_file_format="log",
        # log_component_levels={"Portfolio": "INFO"},
        # ),
        data_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersDataClientConfig(instrument_provider=provider_config, routing=RoutingConfig(default=True)),
        },
        exec_clients={
            "INTERACTIVE_BROKERS": InteractiveBrokersExecClientConfig(instrument_provider=provider_config, routing=RoutingConfig(default=True)),
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

    return node


async def main():
    LoggerAttributes.trading_class = "TradingClass1"
    node = create_trading_node(TraderId("TRADER-001"))
    node.get_event_loop().create_task(node.run_async())

    LoggerAttributes.trading_class = "TradingClass2"
    node = create_trading_node(TraderId("TRADER-002"))
    node.get_event_loop().create_task(node.run_async())
    # nodes[0].get_event_loop().run_forever()
    # nodes[1].get_event_loop().run_forever()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

    # loop = asyncio.get_event_loop()

    # Create threads for each method
    # thread1 = threading.Thread(target=node.run)
    # thread2 = threading.Thread(target=node.run)

    # Start the threads
    # thread1.start()
    # thread2.start()

    # Wait for both threads to finish
    # thread1.join()
    # thread2.join()
