import pandas as pd

from pyfutures.continuous.contract_month import ContractMonth


class TestContractMonth:
    def test_contract_month_init(self):
        # Arrange
        month = ContractMonth("Z21")

        # Act, Arrange
        assert month.year == 2021
        assert month.month == 12
        assert month.value == "Z21"
        assert month.letter_month == "Z"

    def test_contract_month_timestamp_utc(self):
        # Arrange
        month = ContractMonth("Z21")

        # Act, Arrange
        assert month.timestamp_utc == pd.Timestamp("2021-12-01", tz="UTC")

    def test_contract_month_from_month_year(self):
        # Arrange
        month = ContractMonth.from_month_year(2021, 12)

        # Act, Arrange
        assert month.year == 2021
        assert month.month == 12
        assert month.value == "Z21"
        assert month.letter_month == "Z"

    def test_contract_month_to_int(self):
        # Arrange
        month = ContractMonth("Z21")

        # Act, Arrange
        assert month.to_int() == 202112

    def test_contract_month_from_int(self):
        # Arrange
        month = ContractMonth.from_int(202112)

        # Act, Arrange
        assert month.year == 2021
        assert month.month == 12
        assert month.value == "Z21"
        assert month.letter_month == "Z"

    def test_contract_month_equality(self):
        # Arrange
        month = ContractMonth("Z21")

        # Act, Arrange
        assert month == month

    def test_contract_month_hash_str_and_repr(self):
        # Arrange
        month = ContractMonth("Z21")

        # Act, Assert
        assert isinstance(hash(month), int)
        assert repr(month) == "ContractMonth(Z21)"
        assert str(month) == "Z21"
