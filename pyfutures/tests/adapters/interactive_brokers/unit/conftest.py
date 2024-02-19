# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
import json
from pathlib import Path

import pytest

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import Logger
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.common.component import MessageBus
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from pyfutures.adapters.interactive_brokers import IB_VENUE
from pyfutures.adapters.interactive_brokers.client.client import (
    InteractiveBrokersClient,
)
from pyfutures.adapters.interactive_brokers.config import (
    InteractiveBrokersInstrumentProviderConfig,
)
from pyfutures.adapters.interactive_brokers.execution import (
    InteractiveBrokersExecutionClient,
)
from pyfutures.adapters.interactive_brokers.parsing import dict_to_contract_details

# fmt: off
from pyfutures.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider

from pyfutures import IB_ACCOUNT_ID

# @pytest.fixture()
# def event_loop():
#     loop = asyncio.get_event_loop_policy().new_event_loop()
#     asyncio.set_event_loop(loop)
#     yield loop
#     loop.close()
#
# @pytest.fixture()
# def clock():
#     return LiveClock()
#
# @pytest.fixture()
# def logger(clock):
#     return Logger(clock, level_stdout=LogLevel.DEBUG)
#
# @pytest.fixture()
# def msgbus(clock):
#     return MessageBus(
#         TestIdStubs.trader_id(),
#         clock,
#     )
#
#
# @pytest.fixture()
# def cache():
#     cache = TestComponentStubs.cache()
#     return cache
#
# @pytest.fixture()
# def client(event_loop, msgbus, cache, clock, logger) -> InteractiveBrokersClient:
#     client = InteractiveBrokersClient(
#             loop=event_loop,
#             msgbus=msgbus,
#             cache=cache,
#             clock=clock,
#             logger=logger,
#             host="127.0.0.1",
#             port=4002,
#             client_id=1,
#     )
#     return client

@pytest.fixture()
def instrument_provider(event_loop, client, logger, cache) -> InteractiveBrokersInstrumentProvider:

    config = InteractiveBrokersInstrumentProviderConfig()
    
    instrument_provider = InteractiveBrokersInstrumentProvider(
        client=client,
        logger=logger,
        config=config,
    )

    

    return instrument_provider

@pytest.fixture()
def exec_client(event_loop, msgbus, cache, clock, logger, client, instrument_provider) -> InteractiveBrokersExecutionClient:

    account_id = AccountId(f"InteractiveBrokers-{IB_ACCOUNT_ID}")

    # register an exec engine so incoming messages are updating the cache
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
    exec_engine.start()


    cache.add_account(TestExecStubs.margin_account(account_id))
    portfolio = Portfolio(
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
    )

    portfolio.set_specific_venue(IB_VENUE)
    portfolio.update_account(TestEventStubs.margin_account_state())

    exec_client = InteractiveBrokersExecutionClient(
            loop=event_loop,
            client=client,
            account_id=account_id,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            instrument_provider=instrument_provider,
            ibg_client_id=1,
    )

    exec_client._set_account_id(exec_client.account_id)

    exec_engine.register_default_client(exec_client)
    
    # load the contract details json files into the instrument provider
    _load_contracts(instrument_provider)
    
    # add contracts to the cache
    for instrument in instrument_provider.list_all():
        cache.add_instrument(instrument)
            
    return exec_client

def _load_contracts(instrument_provider):
    
    load_ids = {
        "D[X23].ICEEUSOFT",  # near expired contract
        "MES[Z23].CME",  # US liquid contract
        "R[Z23].ICEEU",  # UK liquid contract
        "MDAX[Z23].EUREX",  # UK illiquid contract
        "QM[Z23].NYMEX",  # US illiquid contract
        # "QM[F23].NYMEX",  # US illiquid contract
        # DA, IXV, ALI  # US illiquid contracts
    }
    
    folder = Path(__file__).parent.parent / "responses/import_contracts"
    for instrument_id in load_ids:
        file = folder / (instrument_id + ".json")
        assert file.exists()

        instrument_provider.add_contract_details(
            dict_to_contract_details(json.loads(file.read_text())),
        )

    # check contracts are loaded
    for instrument_id in load_ids:
        assert instrument_provider.find(InstrumentId.from_str(instrument_id)) is not None
