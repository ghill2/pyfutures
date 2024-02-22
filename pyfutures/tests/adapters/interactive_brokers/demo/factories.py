import asyncio

from nautilus_trader.live.data_engine import LiveDataEngine
from nautilus_trader.live.execution_engine import LiveExecutionEngine

from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from pyfutures.adapters.interactive_brokers.factories import InteractiveBrokersLiveDataClientFactory


# Why not use fixtures?
# Some tests require an engine with a custom instrument provider or data_client / exec_client config
# This testing workflows means we can move away from pytest fixtures that don't allow modifying the configs of the classes that need to be instantiated before the fixture runs
# # WIP

DATA_ENGINE = None
EXEC_ENGINE = None


class InteractiveBrokersDataEngineFactory:
    def create(msgbus, cache, clock, client_config: InteractiveBrokersDataClientConfig) -> LiveDataEngine:
        data_client = InteractiveBrokersLiveDataClientFactory.create(
            loop=asyncio.get_event_loop(), name="TESTER", config=client_config, msgbus=msgbus, cache=cache, clock=clock
        )
        data_engine = LiveDataEngine(
            loop=asyncio.get_event_loop(),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        # exec_client._set_account_id(exec_client.account_id)
        data_engine.register_client(data_client)
        data_engine.register_default_client(data_client)

        data_engine.start()

        return data_engine, data_client


def create_exec_engine(event_loop, exec_client, msgbus, cache, clock, logger) -> LiveExecutionEngine:
    """Creates an exec engine"""
    exec_engine = LiveExecutionEngine(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
        config=LiveExecEngineConfig(
            reconciliation=True,
            inflight_check_interval_ms=0,
            debug=True,
        ),
    )
    # exec_client._set_account_id(exec_client.account_id)
    exec_engine.register_client(exec_client)
    exec_engine.register_default_client(exec_client)

    exec_engine.start()

    account_id = AccountId(f"InteractiveBrokers-{IB_ACCOUNT_ID}")

    cache.add_account(TestExecStubs.margin_account(account_id))

    portfolio = Portfolio(
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )

    portfolio.set_specific_venue(IB_VENUE)
    portfolio.update_account(TestEventStubs.margin_account_state())

    return exec_engine
