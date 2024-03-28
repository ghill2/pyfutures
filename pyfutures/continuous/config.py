from typing import Annotated, Literal

import pandas as pd
from msgspec import Meta
from nautilus_trader.common.config import NautilusConfig
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


# An integer constrained to values <= 0
NonPositiveInt = Annotated[int, Meta(le=0)]


class RollConfig(NautilusConfig, frozen=True):
    """
    Configuration for rolls.

    Parameters
    ----------
    hold_cycle : RollCycle
        The contract cycle string we want to hold
    priced_cycle : RollCycle
        The contract cycle string of available prices
    roll_offset : NonPositiveInt
        The day, relative to the expiry date, when we usually roll
    approximate_expiry_offset : int
        The offset, relative to the first day of the contract month that the expiry date approximately occurs.
        After this date, the contract is assumed expired and non-tradeable.
    carry_offset : Literal[1, -1]
        The number of contracts forward or backwards in the priced roll cycle

    """

    hold_cycle: RollCycle
    priced_cycle: RollCycle
    roll_offset: NonPositiveInt
    approximate_expiry_offset: int
    carry_offset: Literal[1, -1]

    def roll_window(
        self,
        month: ContractMonth,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        expiry_date = month.timestamp_utc + pd.Timedelta(
            days=self.approximate_expiry_offset
        )
        roll_date = expiry_date + pd.Timedelta(days=self.roll_offset)
        return (roll_date, expiry_date)

    def __post_init__(self):
        assert self.roll_offset <= 0
        assert self.carry_offset == 1 or self.carry_offset == -1


class ContractChainConfig(NautilusConfig, frozen=True):
    """
    Configuration for contract chain.

    Parameters
    ----------
    bar_type : BarType
        The bar type of the bars that execute the rolls of the chain
    roll_config : RollConfig
        The configuration for the rolls
    start_month : ContractMonth
        The starting month to roll to when started
    """

    instrument_id: InstrumentId
    roll_config: RollConfig
    start_month: ContractMonth
