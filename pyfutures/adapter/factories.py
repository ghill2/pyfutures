import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId

from pyfutures import IB_ACCOUNT_ID
from pyfutures.adapter import IB_VENUE
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.config import InteractiveBrokersExecClientConfig
from pyfutures.adapter.data import InteractiveBrokersDataClient
from pyfutures.adapter.execution import InteractiveBrokersExecClient
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.tests.unit.adapter.stubs import AdapterStubs


CLIENT = None
PROVIDER = None
DATA_CLIENT = None
EXEC_CLIENT = None


def get_provider_cached(client, config):
    global PROVIDER
    if PROVIDER is None:
        PROVIDER = InteractiveBrokersInstrumentProvider(client=client, config=config)
    return PROVIDER


def get_client_cached(loop):
    global CLIENT
    if CLIENT is None:
        CLIENT = InteractiveBrokersClient(
            loop=loop,
            host="127.0.0.1",
            port=4002,
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
        client = get_client_cached(loop)
        provider = get_provider_cached(client, config.instrument_provider)

        global DATA_CLIENT
        if DATA_CLIENT is None:
            DATA_CLIENT = InteractiveBrokersDataClient(
                loop=loop,
                client=client,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                instrument_provider=provider,
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
    ) -> InteractiveBrokersExecClient:
        client = get_client_cached(loop)
        provider = get_provider_cached(client, config.instrument_provider)

        global EXEC_CLIENT
        if EXEC_CLIENT is None:
            EXEC_CLIENT = InteractiveBrokersExecClient(
                loop=loop,
                client=client,
                account_id=AccountId(f"{IB_VENUE.value}-{IB_ACCOUNT_ID}"),
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                instrument_provider=provider,
            )
        return EXEC_CLIENT
