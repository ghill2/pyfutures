import asyncio
import os
from functools import lru_cache
import pytest

# fmt: off
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig


from pyfutures import IB_ACCOUNT_ID
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers.data import InteractiveBrokersDataClient
from pyfutures.adapters.interactive_brokers.execution import InteractiveBrokersExecutionClient
from pyfutures.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider
from pyfutures.adapters.interactive_brokers import IB_VENUE

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import init_logging
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.model.objects import Price


CLIENT = None
PROVIDER = None

PROVIDER_CONFIG = InteractiveBrokersInstrumentProviderConfig(
        chain_filters={
            'FMEU': lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
        },
        parsing_overrides={
            "MIX": {
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        },
    )

def get_provider():
    global PROVIDER
    if PROVIDER is None:
        PROVIDER = InteractiveBrokersInstrumentProvider(
        client=CLIENT,
        config=PROVIDER_CONFIG

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
    Provides a `InteractiveBrokers` live data client factory.
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
        """
        Create a new InteractiveBrokers data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : dict
            The configuration dictionary.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        InteractiveBrokersDataClient

        """
        client = get_client(loop, msgbus, clock, cache)
        provider = get_provider()
        data_client = InteractiveBrokersDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            ibg_client_id=1,
            config=config,
        )
        return data_client


class InteractiveBrokersLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a `InteractiveBrokers` live execution client factory.
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
        provider = get_provider()

        # Set account ID
        # ib_account = config.account_id or os.environ.get("TWS_ACCOUNT")
        # assert ib_account, f"Must pass `{config.__class__.__name__}.account_id` or set `TWS_ACCOUNT` env var."


        # Create client
        exec_client = InteractiveBrokersExecutionClient(
            loop=loop,
            client=client,
            account_id=AccountId(f"{IB_VENUE.value}-{IB_ACCOUNT_ID}"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            ibg_client_id=1
        )
        return exec_client
