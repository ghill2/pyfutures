import pandas as pd

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.cycle import RollCycleRange
from pyfutures.continuous.cycle import RangedRollCycle

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

    def test_cycle_current_month(self):
        
        # Arrange
        cycle = RollCycle("HMUX")

        # Act, Assert
        assert cycle.current_month(pd.Timestamp("2021-10-01")) == ContractMonth("2021X")
        assert cycle.current_month(pd.Timestamp("2021-11-01")) == ContractMonth("2021X")
        assert cycle.current_month(pd.Timestamp("2021-12-01")) == ContractMonth("2022H")

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
    
    def test_cycle_next_month_skips_specified_months(self):
        
        # Arrange
        cycle = RollCycle("HMUX", skip_months=["2022M", "2022U"])
        
        # Act, Assert
        assert cycle.next_month(ContractMonth("2022H")) == ContractMonth("2022X")
        assert cycle.next_month(ContractMonth("2022X")) == ContractMonth("2023H")
        
    def test_cycle_previous_month_skips_specified_months(self):
        
        # Arrange
        cycle = RollCycle("HMUX", skip_months=["2022M", "2022U"])
        
        # Act, Assert
        assert cycle.previous_month(ContractMonth("2022X")) == ContractMonth("2022H")
        assert cycle.previous_month(ContractMonth("2022H")) == ContractMonth("2021X")
        
    def test_ranged_cycle_next_month_returns_expected(self):
        
        # Arrange
        ranges = [
            RollCycleRange(
                start_month=ContractMonth("1998X"),
                end_month=ContractMonth("2014X"),
                cycle=RollCycle("X")
                
            ),
            RollCycleRange(
                start_month=ContractMonth("2015Z"),
                end_month=ContractMonth("2025Z"),
                cycle=RollCycle("Z")
                
            ),
        ]
        cycle = RangedRollCycle(ranges=ranges)

        # Act, Assert
        assert cycle.next_month(ContractMonth("1998X")) == ContractMonth("1999X")
        assert cycle.next_month(ContractMonth("2000X")) == ContractMonth("2001X")
        assert cycle.next_month(ContractMonth("2001X")) == ContractMonth("2002X")
        assert cycle.next_month(ContractMonth("2014X")) == ContractMonth("2015Z")
        assert cycle.next_month(ContractMonth("2014Z")) == ContractMonth("2015Z")
        assert cycle.next_month(ContractMonth("2015F")) == ContractMonth("2015Z")
        assert cycle.next_month(ContractMonth("2015Z")) == ContractMonth("2016Z")
        assert cycle.next_month(ContractMonth("2016Z")) == ContractMonth("2017Z")