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


from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig


class InteractiveBrokersInstrumentProviderConfig(InstrumentProviderConfig, frozen=True):
    chain_filters: dict[str, str] = None
    parsing_overrides: dict[str, str] = None

    # load_contracts: frozenset[IBContract] | None = None
    # load_instrument_ids: frozenset[InstrumentId] | None = None


class InteractiveBrokersDataClientConfig(LiveDataClientConfig, frozen=True):
    pass


class InteractiveBrokersExecClientConfig(LiveExecClientConfig, frozen=True):
    pass
