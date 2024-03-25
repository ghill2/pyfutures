from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth


class ContinuousBarWrangler:
    """
    end_month: ContractMonth
        The end month in the hold cycle the wrangler should stop at. The data
        returned from the wrangler will include the end month (inclusive).
    """

    def __init__(
        self,
        config: ContractChainConfig,
        end_month: ContractMonth,
    ):
        PyCondition.type(config, ContractChainConfig, "config")
        PyCondition.type(end_month, ContractMonth, "end_month")

        self._chain_config = config
        self._end_month = end_month

        self._start_month = self._chain_config.start_month
        assert self._end_month > self._start_month

        self._roll_config = self._chain_config.roll_config
        self._approximate_expiry_offset = self._roll_config.approximate_expiry_offset
        self._roll_offset = self._roll_config.roll_offset

        self._priced_cycle = self._chain_config.roll_config.priced_cycle
        self._carry_offset = self._chain_config.roll_config.carry_offset
        self._hold_cycle = self._chain_config.roll_config.hold_cycle

        self._chain = ContractChain(
            config=self._chain_config,
            clock=TestComponentStubs.clock(),
        )

    def validate(self, bars: list[Bar]) -> None:
        """
        Validate the contract bars for a successful roll from the start to end month.
        """
        venues = {b.bar_type.instrument_id.venue for b in bars}
        assert len(venues) == 1

        specs = {b.bar_type.spec for b in bars}
        assert len(specs) == 1

        try:
            for bar in bars:
                symbol = bar.bar_type.instrument_id.symbol.value
                ContractMonth(symbol.split("=")[-1])
        except AssertionError:
            raise ValueError(
                f"Symbol {symbol} has incorrect format. The format should is <symbol>=<month>",
            )

        timestamps_by_month = {}
        for bar in bars:
            month = bar.bar_type.instrument_id.symbol.value.split("=")[-1]
            if timestamps_by_month.get(month) is None:
                timestamps_by_month[month] = set()
            timestamps_by_month[month].add(bar.ts_init)

        hold_months = self._hold_cycle.get_months(self._start_month, self._end_month)

        missing = [m.value for m in [*hold_months, self._end_month] if timestamps_by_month.get(m.value) is None]

        symbol = self._chain_config.instrument_id.symbol.value
        if len(missing) > 0:
            raise ValueError(f"Data validation failed: {symbol} has no timestamps in months {missing}")

        for current_month in hold_months:
            start, end = self._chain.roll_window(month=current_month)
            start_ns = dt_to_unix_nanos(start)
            end_ns = dt_to_unix_nanos(end)

            # check current contract timestamps exist in roll window
            current_timestamps = {t for t in timestamps_by_month[current_month.value] if t >= start_ns and t < end_ns}
            if len(current_timestamps) == 0:
                raise ValueError(
                    f"Data validation failed: {symbol}={current_month} has no timestamps in roll window {start} to {end}",
                )

            # check forward contract timestamps exist in roll window
            forward_month = self._hold_cycle.next_month(current_month)
            forward_timestamps = {t for t in timestamps_by_month[forward_month.value] if t >= start_ns and t < end_ns}
            if len(forward_timestamps) == 0:
                raise ValueError(
                    f"Data validation failed: {symbol}={forward_month} has no timestamps in roll window {start} to {end}",
                )

            # check matching timestamps for current and forward contract exist in roll window
            is_matching = len(current_timestamps & forward_timestamps) > 0
            if not is_matching:
                raise ValueError(
                    f"Data validation failed: {symbol}: {current_month} and {forward_month} have no matching timestamps in roll window {start} to {end}",
                )

    # def process(
    #     self,
    #     bars: list[Bar],
    # ) -> list[Bar]:
    #     bars = sorted(bars, key=lambda x: x.ts_init)
    #     self.validate(bars)

    #     config = BacktestEngineConfig(
    #         logging=LoggingConfig(bypass_logging=True),
    #         run_analysis=False,
    #     )
    #     engine = BacktestEngine(config=config)

    #     engine.add_data(bars, validate=False)

    #     venue = bars[0].bar_type.instrument_id.venue
    #     engine.add_venue(
    #         venue=venue,
    #         oms_type=OmsType.HEDGING,
    #         account_type=AccountType.MARGIN,
    #         base_currency=USD,
    #         starting_balances=[Money(1_000_000, USD)],
    #     )

    #     results = []
    #     engine.kernel.msgbus.subscribe(
    #         topic=f"data.bars.{self._chain.bar_type}",
    #         handler=results.append,
    #     )

    #     engine.add_actor(self._chain)

    #     symbol = bars[0].bar_type.instrument_id.symbol.value
    #     contracts = TestInstrumentProvider.future(
    #         symbol=symbol,
    #         venue=venue.value,
    #     )
    #     engine.add_instruments(contracts)
    #     engine.run()

    #     engine.dispose()
