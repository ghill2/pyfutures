import pandas as pd
from nautilus_trader.model.data import BarType

from pyfutures.continuous.config import RollConfig


class RollCalendar:
    """
    implement a live and backtest roll calendar for querying chain position

    backtesting

    def get_current_month():
        # get the current month the chain be based on the roll calendar

    does this store current month?
    """

    def __init__(
        self,
        df: pd.DataFrame,
        config: RollConfig,
    ):
        """
        expects df to be in format

        timestamp (UTC), from_month, to_month
        """


class RollCalendarFactory:
    """
    Creates a roll calendar with exact dates

    skip_months : list[ContractMonth], optional
        The months to skip in the hold cycle
    """

    def __init__(
        self,
        bar_type: BarType,
        config: RollConfig,
    ):
        # super().__init__()
        self.bar_type = bar_type
        self.roll_offset = config.roll_offset
        self.carry_offset = config.carry_offset
        self.priced_cycle = config.priced_cycle
        self.hold_cycle = config.hold_cycle

        self._instrument_id = bar_type.instrument_id

    def process(
        self,
    ) -> None:
        current_bar = self.cache.bar(self.current_bar_type)
        if current_bar is None:
            return

        """
        two different datasets:
        """
        start, end = self.roll_window(self.current_month)

        is_expired = self.clock.utc_now() >= end
        if is_expired:
            raise ContractExpired(
                f"The chain failed to roll from {self.current_month} to {self.forward_month} before expiry date {end}",
            )

        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if forward_bar is None:
            return

        current_timestamp = unix_nanos_to_dt(current_bar.ts_init)
        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_init)

        if current_timestamp != forward_timestamp:
            return

        if current_timestamp >= start and current_timestamp < end:
            to_month = self._chain.forward_month
            self._log.info(
                f"Rolling to month {to_month} from {self._chain.current_month}"
            )
            self._chain.set_month(to_month)
