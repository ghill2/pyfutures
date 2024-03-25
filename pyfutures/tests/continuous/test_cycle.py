import pickle

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle


class TestRollCycle:
    def test_cycle_previous_month_returns_expected(self):
        # Arrange
        cycle = RollCycle("HMUX")

        assert cycle.previous_month(ContractMonth("2021Z")) == ContractMonth("2021X")
        assert cycle.previous_month(ContractMonth("2021F")) == ContractMonth("2020X")
        assert cycle.previous_month(ContractMonth("2021V")) == ContractMonth("2021U")

    def test_cycle_next_month_returns_expected(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert cycle.next_month(ContractMonth("2022V")) == ContractMonth("2022X")
        assert cycle.next_month(ContractMonth("2021Z")) == ContractMonth("2022H")
        assert cycle.next_month(ContractMonth("2021F")) == ContractMonth("2021H")

    def test_cycle_contains(self):
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert ContractMonth("2020X") in cycle

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

    def test_cycle_pickle(self):
        cycle = RollCycle("HMUZ")
        pickled = pickle.dumps(cycle)
        unpickled = pickle.loads(pickled)  # noqa S301 (pickle is safe here)

        # Assert
        assert unpickled == cycle
