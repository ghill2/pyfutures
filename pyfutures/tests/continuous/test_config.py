from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.pyfutures.continuous.config import FuturesChainConfig


class TestRollConfig:
    def setup(self):
        self.params = FuturesChainConfig(
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset=-45,
            approximate_expiry_offset=14,
            carry_offset=1,
        )

    def test_from_dict(self):
        # Arrange
        value = {
            "hold_cycle": "FGHJKMNQUVXZ",
            "priced_cycle": "FGHJKMNQUVXZ",
            "roll_offset": -45,
            "expiry_offset": 14,
            "carry_offset": 1,
        }

        # Act
        parameters = FuturesChainConfig.from_dict(value)

        # Assert
        assert parameters.hold_cycle == RollCycle("FGHJKMNQUVXZ")
        assert parameters.priced_cycle == RollCycle("FGHJKMNQUVXZ")
        assert parameters.roll_offset == -45
        assert parameters.approximate_expiry_offset == 14
        assert parameters.carry_offset == 1
