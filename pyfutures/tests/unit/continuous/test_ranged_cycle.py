import pytest

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.cycle_range import RangedRollCycle
from pyfutures.continuous.cycle_range import RollCycleRange


class TestRangedCycle:
    def test_ranged_cycle_previous_month_returns_expected(self):
        # Arrange
        ranges = [
            RollCycleRange(
                start_month=None, end_month=ContractMonth("2014X"), cycle=RollCycle("X")
            ),
            RollCycleRange(
                start_month=ContractMonth("2015Z"), end_month=None, cycle=RollCycle("Z")
            ),
        ]
        cycle = RangedRollCycle(ranges=ranges)

        # Act, Assert
        assert cycle.previous_month(ContractMonth("2013X")) == ContractMonth(
            "2014X"
        )  # in 1st range
        assert cycle.previous_month(ContractMonth("2014X")) == ContractMonth(
            "2013X"
        )  # 1st range end
        assert cycle.previous_month(ContractMonth("2015Z")) == ContractMonth(
            "2014X"
        )  # 2nd range start
        assert cycle.previous_month(ContractMonth("2016Z")) == ContractMonth(
            "2015Z"
        )  # in 2nd range
        assert cycle.previous_month(ContractMonth("2017F")) == ContractMonth(
            "2016Z"
        )  # in 2nd range
        assert cycle.previous_month(ContractMonth("2015X")) == ContractMonth(
            "2014X"
        )  # inbetween

    def test_ranged_cycle_next_month_returns_expected(self):
        # Arrange
        ranges = [
            RollCycleRange(
                start_month=None, end_month=ContractMonth("2014X"), cycle=RollCycle("X")
            ),
            RollCycleRange(
                start_month=ContractMonth("2015Z"), end_month=None, cycle=RollCycle("Z")
            ),
        ]
        cycle = RangedRollCycle(ranges=ranges)

        # Act, Assert

        assert cycle.next_month(ContractMonth("2013X")) == ContractMonth(
            "2014X"
        )  # in 1st range
        assert cycle.next_month(ContractMonth("2014X")) == ContractMonth(
            "2015Z"
        )  # 1st range end
        assert cycle.next_month(ContractMonth("2014Z")) == ContractMonth(
            "2015Z"
        )  # inbetween
        assert cycle.next_month(ContractMonth("2015F")) == ContractMonth(
            "2015Z"
        )  # inbetween
        assert cycle.next_month(ContractMonth("2015Z")) == ContractMonth(
            "2016Z"
        )  # 2nd range start
        assert cycle.next_month(ContractMonth("2016Z")) == ContractMonth(
            "2017Z"
        )  # in 2nd range

    @pytest.mark.skip(reason="TODO")
    def test_ranged_cycle_contains(self):
        pass

    @pytest.mark.skip(reason="TODO")
    def test_cycle_equality(self):
        pass

    def test_ranged_roll_cycle_from_str(self):
        value = "2014X=X,2015Z=Z"

        cycle = RangedRollCycle.from_str(value)

        assert cycle.ranges == [
            RollCycleRange(
                start_month=None,
                end_month=ContractMonth("2014X"),
                cycle=RollCycle("X"),
            ),
            RollCycleRange(
                start_month=ContractMonth("2015Z"),
                end_month=None,
                cycle=RollCycle("Z"),
            ),
        ]

    @pytest.mark.skip(reason="TODO")
    def test_ranged_cycle_next_month_skips_specified_months(self):
        pass

    @pytest.mark.skip(reason="TODO")
    def test_ranged_cycle_previous_month_skips_specified_months(self):
        pass

    @pytest.mark.skip(reason="TODO")
    def test_ranged_cycle_pickle(self):
        pass
