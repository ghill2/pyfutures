import asyncio

from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.data_engine import LiveDataEngine
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from pyfutures import IB_ACCOUNT_ID
from pyfutures.adapter import IB_VENUE

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.adapter.config import (
    InteractiveBrokersDataClientConfig,
    InteractiveBrokersExecClientConfig,
    InteractiveBrokersInstrumentProviderConfig,
)
from pyfutures.adapter.execution import (
    InteractiveBrokersExecClient,
)
from pyfutures.adapter.factories import (
    InteractiveBrokersLiveDataClientFactory,
)
from pyfutures.adapter.providers import (
    InteractiveBrokersInstrumentProvider,
)
import logging

# Why not use fixtures?
# Some tests require an engine with a custom instrument provider or data_client / exec_client config
# This testing workflows means we can move away from pytest fixtures that don't allow modifying the configs of the classes that need to be instantiated before the fixture runs
# # WIP
# session=True or session=False for testing is decided in the fixtures


class InteractiveBrokersClientFactory:
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
        log_level: int = logging.DEBUG,
        api_log_level: int = logging.INFO,
    ) -> InteractiveBrokersClient:
        return InteractiveBrokersClient(
            loop=loop,
            host=host,
            port=port,
            client_id=client_id,
            log_level=log_level,
            api_log_level=api_log_level,
        )


class InteractiveBrokersDataEngineFactory:
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        msgbus,
        cache,
        clock,
        data_client_config: InteractiveBrokersDataClientConfig | None = None,
    ) -> LiveDataEngine:
        if data_client_config is None:
            data_client_config = InteractiveBrokersDataClientConfig()

        data_client = InteractiveBrokersLiveDataClientFactory.create(
            loop=loop,
            name="TESTER",
            config=data_client_config,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        data_engine = LiveDataEngine(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        # exec_client._set_account_id(exec_client.account_id)
        data_engine.register_client(data_client)
        data_engine.register_default_client(data_client)

        data_engine.start()

        return data_engine, data_client


class InteractiveBrokersExecEngineFactory:
    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        msgbus,
        cache,
        clock,
        provider_config: InteractiveBrokersInstrumentProviderConfig | None = None,
        exec_client_config: InteractiveBrokersExecClientConfig | None = None,
    ) -> tuple[
        LiveExecutionEngine,
        InteractiveBrokersExecClient,
        InteractiveBrokersInstrumentProvider,
        InteractiveBrokersClient,
    ]:
        if provider_config is None:
            provider_config = InteractiveBrokersInstrumentProviderConfig()

        if exec_client_config is None:
            exec_client_config = InteractiveBrokersExecClientConfig()

        client = InteractiveBrokersClient(
            loop=loop,
            host="127.0.0.1",
            port=4002,
        )

        provider = InteractiveBrokersInstrumentProvider(
            client=client, config=provider_config
        )

        account_id = AccountId(f"{IB_VENUE.value}-{IB_ACCOUNT_ID}")

        exec_client = InteractiveBrokersExecClient(
            loop=loop,
            client=client,
            account_id=account_id,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            ibg_client_id=1,
        )

        exec_client._set_account_id(exec_client.account_id)

        exec_engine = LiveExecutionEngine(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=LiveExecEngineConfig(
                reconciliation=True,
                inflight_check_interval_ms=0,
                debug=True,
            ),
        )

        exec_engine.register_client(exec_client)
        exec_engine.register_default_client(exec_client)

        exec_engine.start()

        cache.add_account(TestExecStubs.margin_account(account_id))

        portfolio = Portfolio(
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        portfolio.set_specific_venue(IB_VENUE)
        portfolio.update_account(TestEventStubs.margin_account_state())

        return exec_engine, exec_client, provider, client
