import asyncio

# fmt: off
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId

from pyfutures import IB_ACCOUNT_ID
from pyfutures.adapter import IB_VENUE
from pyfutures.adapter.client.client import InteractiveBrokersClient
from pyfutures.adapter.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapter.data import InteractiveBrokersDataClient
from pyfutures.adapter.execution import InteractiveBrokersExecutionClient
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider


CLIENT = None
PROVIDER = None
DATA_CLIENT = None
EXEC_CLIENT = None



def get_provider(config: InteractiveBrokersInstrumentProviderConfig):
    global PROVIDER
    if PROVIDER is None:
        PROVIDER = InteractiveBrokersInstrumentProvider(
        client=CLIENT,
        config=config,
    )
    return PROVIDER



def get_client(loop, msgbus, clock, cache):
    # COPIED FROM PYTEST TEST FIXTURES
    global CLIENT
    if CLIENT is None:
        CLIENT = InteractiveBrokersClient(
        loop=loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        host="127.0.0.1",
        port=4002,
        client_id=1,
        )
    return CLIENT

# fmt: on
class InteractiveBrokersLiveDataClientFactory(LiveDataClientFactory):
    """
    Called from the TradingNode with fixed arguments
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: InteractiveBrokersDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> InteractiveBrokersDataClient:
        client = get_client(loop, msgbus, clock, cache)
        provider = get_provider(config=config.instrument_provider)

        global DATA_CLIENT
        if DATA_CLIENT is None:
            DATA_CLIENT = InteractiveBrokersDataClient(
                loop=loop,
                client=client,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                instrument_provider=provider,
                ibg_client_id=1,
                config=config,
            )
        return DATA_CLIENT


class InteractiveBrokersLiveExecClientFactory(LiveExecClientFactory):
    """
    Called from the TradingNode with fixed arguments
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: InteractiveBrokersExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> InteractiveBrokersExecutionClient:
        client = get_client(loop, msgbus, clock, cache)
        provider = get_provider(config=config.instrument_provider)

        global EXEC_CLIENT
        if EXEC_CLIENT is None:
            EXEC_CLIENT = InteractiveBrokersExecutionClient(
                loop=loop,
                client=client,
                account_id=AccountId(f"{IB_VENUE.value}-{IB_ACCOUNT_ID}"),
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                instrument_provider=provider,
                ibg_client_id=1,
            )
        return EXEC_CLIENT
