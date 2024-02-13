import pickle

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.multiple_bar import MultipleBar
from nautilus_trader.model.objects import Price
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar

class TestMultipleBar:
    
    def setup_method(self):
        self.price1 = MultipleBar(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_bar=Bar(
                bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("1.1"),
                high=Price.from_str("1.3"),
                low=Price.from_str("1.0"),
                close=Price.from_str("1.2"),
                volume=Quantity.from_int(1),
                ts_init=0,
                ts_event=0,
            ),
            forward_bar=Bar(
                bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("2.1"),
                high=Price.from_str("2.3"),
                low=Price.from_str("2.0"),
                close=Price.from_str("2.2"),
                volume=Quantity.from_int(2),
                ts_init=0,
                ts_event=0,
            ),
            carry_bar=Bar(
                bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("3.1"),
                high=Price.from_str("3.3"),
                low=Price.from_str("3.0"),
                close=Price.from_str("3.2"),
                volume=Quantity.from_int(3),
                ts_init=0,
                ts_event=0,
            ),
            ts_event=0,
            ts_init=0,
        )
        
        self.price2 = MultipleBar(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_bar=Bar(
                bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("1.1"),
                high=Price.from_str("1.3"),
                low=Price.from_str("1.0"),
                close=Price.from_str("1.2"),
                volume=Quantity.from_int(1),
                ts_event=0,
                ts_init=0,
            ),
            forward_bar=None,
            carry_bar=None,
            ts_event=0,
            ts_init=0,
        )
        
    def test_continuous_price_equality(self):
        
        # Arrange, Act, Assert
        assert self.price1 == self.price1
        assert self.price2 == self.price2

    def test_continuous_price_str_and_repr(self):
        
        # Arrange, Act, Assert
        assert (
            str(self.price1)
            == "MultipleBar(bar_type=MES.SIM-1-DAY-MID-EXTERNAL, current_bar=MES=2021X.SIM-1-DAY-MID-EXTERNAL,1.1,1.3,1.0,1.2,1,0, forward_bar=MES=2021Z.SIM-1-DAY-MID-EXTERNAL,2.1,2.3,2.0,2.2,2,0, carry_bar=MES=2021Z.SIM-1-DAY-MID-EXTERNAL,3.1,3.3,3.0,3.2,3,0, ts_event=0, ts_init=0)"  # noqa
        )
        assert (
            repr(self.price1)
            == "MultipleBar(bar_type=MES.SIM-1-DAY-MID-EXTERNAL, current_bar=MES=2021X.SIM-1-DAY-MID-EXTERNAL,1.1,1.3,1.0,1.2,1,0, forward_bar=MES=2021Z.SIM-1-DAY-MID-EXTERNAL,2.1,2.3,2.0,2.2,2,0, carry_bar=MES=2021Z.SIM-1-DAY-MID-EXTERNAL,3.1,3.3,3.0,3.2,3,0, ts_event=0, ts_init=0)"  # noqa
        )

    def test_to_dict(self):
        
        # Act
        values = MultipleBar.to_dict(self.price1)

        # Assert
        assert values == {'bar_type': 'MES.SIM-1-DAY-MID-EXTERNAL', 'current_bar_type': 'MES=2021X.SIM-1-DAY-MID-EXTERNAL', 'current_open': '1.1', 'current_high': '1.3', 'current_low': '1.0', 'current_close': '1.2', 'current_volume': '1', 'current_ts_event': 0, 'current_ts_init': 0, 'forward_bar_type': 'MES=2021Z.SIM-1-DAY-MID-EXTERNAL', 'forward_open': '2.1', 'forward_high': '2.3', 'forward_low': '2.0', 'forward_close': '2.2', 'forward_volume': '2', 'forward_ts_event': 0, 'forward_ts_init': 0, 'carry_bar_type': 'MES=2021Z.SIM-1-DAY-MID-EXTERNAL', 'carry_open': '3.1', 'carry_high': '3.3', 'carry_low': '3.0', 'carry_close': '3.2', 'carry_volume': '3', 'carry_ts_event': 0, 'carry_ts_init': 0, 'ts_event': 0, 'ts_init': 0}  # noqa

    def test_from_dict_returns_expected_price(self):

        # Arrange, Act
        result = MultipleBar.from_dict(MultipleBar.to_dict(self.price1))

        # Assert
        assert result == self.price1

    def test_pickle_bar(self):

        # Arrange, Act
        pickled = pickle.dumps(self.price1)
        unpickled = pickle.loads(pickled)  # noqa S301 (pickle is safe here)

        # Assert
        assert unpickled == self.price1

    def test_continuous_price_serialize_roundtrip(self):
        
        # Arrange
        register_arrow(
            data_cls=MultipleBar,
            schema=MultipleBar.schema(),
            encoder=make_dict_serializer(schema=MultipleBar.schema()),
            decoder=make_dict_deserializer(data_cls=MultipleBar),
        )

        # Act
        serialized = ArrowSerializer.serialize(self.price1, data_cls=MultipleBar)
        deserialized = ArrowSerializer.deserialize(data_cls=MultipleBar, batch=serialized)

        # Assert
        assert deserialized[0] == self.price1
