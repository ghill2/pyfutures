# init_logging(level_stdout=LogLevel.DEBUG)

from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.common import Environment
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import RoutingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Price
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.factories import InteractiveBrokersLiveDataClientFactory
from pyfutures.adapter.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.tests.unit.adapter.stubs import AdapterStubs as UnitAdapterStubs


class AdapterStubs:
    @staticmethod
    def trading_node(loop, trader_id: TraderId, load_ids: list):
        provider_config_dict = dict(
            load_ids=load_ids,
            **UnitAdapterStubs.provider_config(),
        )
        provider_config = InteractiveBrokersInstrumentProviderConfig(**provider_config_dict)
        # provider_config = InteractiveBrokersInstrumentProviderConfig(
        #     chain_filters={
        #         "FMEU": lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
        #     },
        #     parsing_overrides={
        #         "MIX": {
        #             "price_precision": 0,
        #             "price_increment": Price(5, 0),
        #         },
        #     },
        # )

        config = TradingNodeConfig(
            trader_id=trader_id,
            environment=Environment.LIVE,
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

        node.portfolio.set_specific_venue(IB_VENUE)

        return node