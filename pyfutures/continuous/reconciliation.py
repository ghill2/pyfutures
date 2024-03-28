import pandas as pd
from nautilus_trader.model.position import Position

from pyfutures.continuous.contract_month import ContractMonth


class Reconciliation:
    def reconcile_month(self) -> ContractMonth:
        positions = self.cache.positions(
            strategy_id=self._strategy_id,
        )
        if len(positions) == 0 or len(self.continuous) == 0:
            self._log.info("Finding month from calendar")
            return self._reconcile_month_from_calendar(self.clock.utc_now())

        elif len(positions) == 1:
            self._log.info("Finding month from position")
            return self._reconcile_month_from_position(positions[0])

        elif len(positions) > 1:
            raise RuntimeError(
                f"IB has more than one position for {self._instrument_id}"
            )

    def _reconcile_month_from_position(self, position: Position) -> ContractMonth:
        instrument = self.cache.instrument(position.instrument_id)
        assert instrument is not None

        position_month = ContractMonth(instrument.id.symbol.value.split("=")[-1])
        last_month = self.continuous[-1].current_month
        if position_month != last_month:
            raise RuntimeError(
                f"Position has month {position_month} but last month of cached data is {last_month}"
            )

        return last_month

    def _reconcile_month_from_calendar(self, now: pd.Timestamp) -> ContractMonth:
        df = pd.DataFrame()
        df["month"] = self.hold_cycle.get_months(
            start=ContractMonth(f"{now.year - 2}{self.hold_cycle.value[0]}"),
            end=ContractMonth(f"{now.year + 2}{self.hold_cycle.value[0]}"),
        )
        df["start"] = [self._roll_window(month)[0] for month in df.month]
        df["end"] = [self._roll_window(month)[1] for month in df.month]

        mask = now < df.end
        return df[mask].iloc[0].month

        # mask = (now >= df.start) & (now < df.end)
        # inside = df[mask]

        # if inside.empty:
        #     # previous if outside roll window
        #     mask = (now > df.end)
        #     self.current_month = df[mask].iloc[0].month
        # else:
        #     # current month if inside roll window
        #     # assert len(inside) == 1
        #     # idx = inside.iloc[0].index + 1
        #     self.current_month = inside.iloc[0].month

    def reconcile_data(self) -> None:
        assert self.current_month is not None

    # def reconcile(self) -> None:

    #     self._log.info(f"Reconciling continuous data")

    #     self.cache.load_actor(self)

    #     start_month = self.reconcile_month()
    #     self._log.info(f"start_month: {start_month}")

    #     self.roll(start_month)

    #     self.reconcile_data()

    #     # self.roll(self.forward_month)
