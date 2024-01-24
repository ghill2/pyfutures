from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.config.validation import NonNegativeInt
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.schedule.schedule import MarketSchedule
from nautilus_trader.config.common import NautilusConfig

from typing import Annotated

from msgspec import Meta
from typing import Annotated, Literal

# An integer constrained to values <= 0
NonPositiveInt = Annotated[int, Meta(le=0)]

class ContractChainConfig(NautilusConfig, frozen=True):
    """
    Represents the config for a FutureChain.

    hold_cycle: The contract cycle string we want to hold
    priced_cycle: The contract cycle string of available prices
    roll_offset: The day, relative to the expiry date, when we usually roll
    carry_offset: The number of contracts forward or backwards defines carry in the priced roll cycle
    approximate_expiry_offset: The offset, relative to the first of the contract month that the expiry date approximately occurs

    """

    instrument_id: InstrumentId
    hold_cycle: RollCycle
    priced_cycle: RollCycle
    roll_offset: NonPositiveInt
    approximate_expiry_offset: NonNegativeInt
    carry_offset: Literal[1, -1]
    skip_months: list[ContractMonth] | None = None
    weekly_schedule: MarketSchedule | None = None
