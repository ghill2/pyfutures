from typing import Annotated, Literal

from msgspec import Meta
from nautilus_trader.common.config import NautilusConfig
from nautilus_trader.common.config import NonNegativeInt
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


# An integer constrained to values <= 0
NonPositiveInt = Annotated[int, Meta(le=0)]


class RollConfig(NautilusConfig, frozen=True):
    """
    Configuration for the rolling of contracts

    Parameters
    ----------
    hold_cycle : RollCycle, The contract cycle string we want to hold
    priced_cycle : RollCycle, The contract cycle string of available prices
    roll_offset : NonPositiveInt, The day, relative to the expiry date, when we usually roll
    carry_offset : int, The number of contracts forward or backwards defines carry in the priced roll cycle
    approximate_expiry_offset : The offset, relative to the first of the contract month that the expiry date approximately occurs
        The cache configuration.
    """

    instrument_id: InstrumentId
    hold_cycle: RollCycle
    priced_cycle: RollCycle
    roll_offset: NonPositiveInt
    approximate_expiry_offset: NonNegativeInt
    carry_offset: Literal[1, -1]
    skip_months: list[ContractMonth] | None = None


class ContractChainConfig(NautilusConfig, frozen=True):
    bar_type: BarType
    roll_config: RollConfig
    raise_expired: bool = True
    ignore_expiry_date: bool = False
    maxlen: int | None = None
    start_month: ContractMonth | None = None
