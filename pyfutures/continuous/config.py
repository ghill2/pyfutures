from dataclasses import dataclass


@dataclass
class FuturesContractChainConfig:
    """
    Represents the config for a FutureChain.

    hold_cycle: The contract cycle string we want to hold
    priced_cycle: The contract cycle string of available prices
    roll_offset: The day, relative to the expiry date, when we usually roll
    carry_offset: The number of contracts forward or backwards defines carry in the priced roll cycle
    approximate_expiry_offset: The offset, relative to the first of the contract month that the expiry date approximately occurs

    """

    instrument_id: str
    hold_cycle: str
    priced_cycle: str
    roll_offset: int
    approximate_expiry_offset: int
    carry_offset: int
    skip_months: list[str] | None = None
