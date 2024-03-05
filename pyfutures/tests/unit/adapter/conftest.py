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

import json
from pathlib import Path

import pytest

from pyfutures.adapter.execution import InteractiveBrokersExecutionClient
from pyfutures.adapter.parsing import dict_to_contract_details
from pyfutures.tests.demo.adapter.factories import InteractiveBrokersExecEngineFactory

# fmt: off

@pytest.fixture()
def exec_client(event_loop, msgbus, cache, clock) -> InteractiveBrokersExecutionClient:
    _, exec_client, provider, _  = InteractiveBrokersExecEngineFactory.create(loop=event_loop, msgbus=msgbus, cache=cache, clock=clock)


    # load the contract details json files into the instrument provider
    _load_contracts(provider)

    # add contracts to the cache
    for instrument in provider.list_all():
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
