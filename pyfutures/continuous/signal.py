from typing import Callable

class RollSignal:
    def __init__(
        self,
        handlers: set[Callable],
    ):
    # in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)
    if self._ignore_expiry_date:
        in_roll_window = (current_timestamp >= self.roll_date)
    else:
        current_day = current_timestamp.floor("D")
        in_roll_window = (current_timestamp >= self.roll_date) and (current_day <= self.expiry_day)