from __future__ import annotations

from dataclasses import dataclass

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


@dataclass
class RollCycleRange:
    start_month: ContractMonth | None
    end_month: ContractMonth | None
    cycle: RollCycle

    def __contains__(self, month: ContractMonth) -> bool:
        return (self.start_month is None or month >= self.start_month) and (self.end_month is None or month <= self.end_month)


@dataclass
class RangedRollCycle:
    # TODO: check ranges are not overlapping
    # TODO: check no gap between ranges
    # TODO: check start range can only have None start_month
    # TODO: check end range can only have None end_month
    # TODO: start end can't be the same
    # TODO first range needs start month as None
    # TODO last range needs end month as None

    ranges: list[RollCycleRange]

    @staticmethod
    def from_str(
        value: str,
        skip_months: list[str] | None = None,
    ) -> RangedRollCycle:
        parts = value.strip().replace("", "").split(",")
        assert len(parts) == 2

        # parse start
        value = parts.pop(0)
        ranges = [
            RollCycleRange(
                start_month=None,
                end_month=ContractMonth(value.split("=")[0]),
                cycle=RollCycle(value.split("=")[1], skip_months=skip_months),
            )
        ]

        # parse start
        value = parts.pop(-1)
        ranges.append(
            RollCycleRange(
                start_month=ContractMonth(value.split("=")[0]),
                end_month=None,
                cycle=RollCycle(value.split("=")[1], skip_months=skip_months),
            )
        )

        # TODO parse mid
        # if len(parts) > 1:
        #     for value in parts:
        #         ranges.insert(
        #             1,
        #             RollCycleRange(
        #                 start_month=ContractMonth(value.split(">")[0]),
        #                 end_month=ContractMonth(value.split(">")[1].split("=")[0]),
        #                 cycle=RollCycle(value.split(">")[1].split("=")[1], skip_months=skip_months),
        #             ),
        #         )

        return RangedRollCycle(ranges=ranges)

    def __contains__(self, month: ContractMonth) -> bool:
        for r in self.ranges:
            if month in r and month in r.cycle:
                return True
        return False

    def next_month(self, current: ContractMonth) -> ContractMonth:
        for i, r in enumerate(self.ranges):
            if i != len(self.ranges) - 1:
                next_r = self.ranges[i + 1]
                if current >= r.end_month and current < next_r.start_month:
                    return next_r.start_month

            if current in r:
                return r.cycle.next_month(current=current)

        raise RuntimeError  # design-time error

    def previous_month(self, current: ContractMonth) -> ContractMonth:
        for i, r in enumerate(self.ranges):
            if i > 0:
                last = self.ranges[i - 1]
                if current > last.end_month and current <= r.start_month:
                    return last.end_month

            if current in r:
                return r.cycle.previous_month(current=current)

        raise RuntimeError  # design-time error

    def get_months(self, start: ContractMonth, end: ContractMonth) -> set[ContractMonth]:
        months = set()
        assert start in self
        while start < end:
            months.add(start)
            start = self.next_month(start)
        return months
