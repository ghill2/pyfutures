import pandas as pd

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


class TestRollCycle:
    def test_cycle_closest_previous_returns_expected(self):
        # Arrange
        cycle = RollCycle("HMUX")

        assert cycle.previous_month(ContractMonth("Z21")) == ContractMonth("X21")
        assert cycle.previous_month(ContractMonth("F21")) == ContractMonth("X20")
        assert cycle.previous_month(ContractMonth("V21")) == ContractMonth("U21")

    def test_cycle_closest_next_returns_expected(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert cycle.next_month(ContractMonth("V22")) == ContractMonth("X22")
        assert cycle.next_month(ContractMonth("Z21")) == ContractMonth("H22")
        assert cycle.next_month(ContractMonth("F21")) == ContractMonth("H21")

    def test_cycle_current_month(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert cycle.current_month(pd.Timestamp("2021-10-01")) == ContractMonth("X21")
        assert cycle.current_month(pd.Timestamp("2021-11-01")) == ContractMonth("X21")
        assert cycle.current_month(pd.Timestamp("2021-12-01")) == ContractMonth("H22")

    def test_cycle_contains(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert "X" in cycle
        assert 11 in cycle

    def test_cycle_equality(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert cycle == cycle

    def test_cycle_str_len_repr(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert len(cycle) == 4
        assert str(cycle) == "HMUX"
        assert repr(cycle) == "RollCycle(HMUX)"
