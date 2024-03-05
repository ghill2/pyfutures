from pyfutures.continuous.config import RollConfig
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.identifiers import InstrumentId


class TestRollConfig:
    def setup(self):
        self.params = RollConfig(
            instrument_id=InstrumentId.from_str("TEST.SIM"),
            hold_cycle="FGHJKMNQUVXZ",
            priced_cycle="FGHJKMNQUVXZ",
            roll_offset=-45,
            approximate_expiry_offset=14,
            carry_offset=1,
        )
    
    
    # def test_from_dict(self):
    #     # Arrange
    #     value = {
    #         "instrument_id": "TEST.SIM",
    #         "hold_cycle": "FGHJKMNQUVXZ",
    #         "priced_cycle": "FGHJKMNQUVXZ",
    #         "roll_offset": -45,
    #         "expiry_offset": 14,
    #         "carry_offset": 1,
    #     }

    #     # Act
    #     parameters = RollConfig.from_dict(value)

    #     # Assert
    #     assert parameters.hold_cycle == RollCycle("FGHJKMNQUVXZ")
    #     assert parameters.priced_cycle == RollCycle("FGHJKMNQUVXZ")
    #     assert parameters.roll_offset == -45
    #     assert parameters.approximate_expiry_offset == 14
    #     assert parameters.carry_offset == 1
