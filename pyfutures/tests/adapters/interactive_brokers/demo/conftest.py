import asyncio

import pytest
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Price
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs

# fmt: off
from pyfutures.adapters.interactive_brokers import IB_VENUE
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersInstrumentProviderConfig
from pyfutures.adapters.interactive_brokers.execution import InteractiveBrokersExecutionClient
from pyfutures.adapters.interactive_brokers.factories import InteractiveBrokersLiveExecClientFactory
from pyfutures.tests.adapters.order_setup import OrderSetup

initialize_log
def pytest_addoption(parser):
    parser.addoption(
        '--instrument-id',
        action='store',
        default="EOE-MFA-Z23.FTA",
        help='Base URL for the API tests',
    )
    parser.addoption(
        '--file-logging',
        action='store',
        default=False,
        help='Enable file logging for the test',
    )
    parser.addoption(
        '--file-log-path',
        action='store',
        default="",
        help='Log path for the test',
    )

@pytest.fixture(scope="session")
def instrument_id(request) -> InstrumentId:
    value = request.config.getoption('--instrument-id')
    return InstrumentId.from_str(value)

@pytest.fixture(scope="session")
def instrument(event_loop, cache, instrument_provider, instrument_id) -> FuturesContract:

    instrument = event_loop.run_until_complete(
        instrument_provider.load_async(instrument_id),
    )

    if instrument is None:
        for instrument in instrument_provider.list_all():
            print(instrument)
        raise RuntimeError(f"Instrument not found: {instrument_id}")

    cache.add_instrument(instrument)

    return instrument










@pytest.fixture()
def provider_params():
    return dict(
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


@pytest.fixture(scope="session")
def exec_client(provider_params, event_loop, msgbus, cache, clock) -> InteractiveBrokersExecutionClient:

    # always returns the same instance of the client
    InteractiveBrokersLiveExecClientFactory.create(
        loop=event_loop,
        name="TEST",
        config=InteractiveBrokersExecClientConfig(
            instrument_provider=InteractiveBrokersInstrumentProviderConfig(
                **provider_params,
            )
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock
    )

    # return InteractiveBrokersExecutionClient(
    #         loop=event_loop,
    #         client=client,
    #         account_id=AccountId(f"InteractiveBrokers-{IB_ACCOUNT_ID}"),
    #         msgbus=msgbus,
    #         cache=cache,
    #         clock=clock,
    #         instrument_provider=instrument_provider,
    #         ibg_client_id=1,
    # )


@pytest.fixture(scope="session")
def exec_engine(event_loop, exec_client, msgbus, cache, clock, logger) -> LiveExecutionEngine:

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



# @pytest.fixture(scope="session")
# def trading_node() -> TradingNode:
#     # Arrange
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#
#     # monkeypatch.setenv("BINANCE_API_KEY", "SOME_API_KEY")
#     # monkeypatch.setenv("BINANCE_API_SECRET", "SOME_API_SECRET")
#
#     config = TradingNodeConfig(
#         logging=LoggingConfig(bypass_logging=True),
#         environment=Environment.Live,
#         data_clients={
#             "BINANCE": BinanceDataClientConfig(
#                 instrument_provider=InstrumentProviderConfig(load_all=False),
#             ),
#         },
#         exec_clients={
#             "BINANCE": BinanceExecClientConfig(
#                 instrument_provider=InstrumentProviderConfig(load_all=False),
#             ),
#         },
#         timeout_disconnection=1.0,  # Short timeout for testing
#         timeout_post_stop=1.0,  # Short timeout for testing
#     )
#     node = TradingNode(config=config, loop=loop)
#
#     node.add_data_client_factory("INTERACTIVE_BROKERS", BinanceLiveDataClientFactory)
#     node.add_exec_client_factory("BINANCE", BinanceLiveExecClientFactory)


@pytest.fixture(scope="session")
def order_setup(event_loop, exec_client, exec_engine) -> OrderSetup:
    order_setup = OrderSetup(
        exec_client=exec_client,
        data_client=None,
    )
    event_loop.run_until_complete(order_setup.close_all())
    event_loop.run_until_complete(asyncio.sleep(1))
    yield order_setup
    event_loop.run_until_complete(order_setup.close_all())

# @pytest.fixture(scope="session")
# def socket(event_loop) -> InteractiveBrokersClient:
#     return Socket(
#             loop=event_loop,
#             host="127.0.0.1",
#             port=4002,
#             client_id=1,
#             callback=None,
#     )

# @pytest.fixture()
# def delay() -> OrderSetup:
#     # asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.25))
#     return

# @pytest.fixture(scope="session")
# def log(request, clock, instrument_id) -> None:
#     file_logging = request.config.getoption('--file-logging')
#     file_log_path = request.config.getoption('--file-log-path')





# @pytest.fixture(scope="session")
# def log(logger) -> Logger:
#     return Logger("pytest")
