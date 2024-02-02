import pickle

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.model.objects import Price
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.model.data import BarType

class TestMultiplePrice:
    def test_continuous_price_equality(self):
        # Arrange
        price1 = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=Price.from_str("1.0"),
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=Price.from_str("1.0"),
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )
        price2 = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=Price.from_str("1.0"),
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            current_price=None,
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=None,
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert price1 == price1
        assert price2 == price2

    def test_continuous_price_str_and_repr(self):
        # Arrange
        price = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=Price.from_str("1.0"),
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=Price.from_str("1.0"),
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert (
            str(price)
            == "MultiplePrice(bar_type=MES.SIM-1-DAY-MID-EXTERNAL, current_price=1.1, current_bar_type=MES=2021X.SIM-1-DAY-MID-EXTERNAL, forward_price=1.0, forward_bar_type=MES=2021Z.SIM-1-DAY-MID-EXTERNAL, carry_price=1.0, carry_bar_type=MES=2021Z.SIM-1-DAY-MID-EXTERNAL, ts_event=0, ts_init=0)"  # noqa
        )
        assert (
            repr(price)
            == "MultiplePrice(bar_type=MES.SIM-1-DAY-MID-EXTERNAL, current_price=1.1, current_bar_type=MES=2021X.SIM-1-DAY-MID-EXTERNAL, forward_price=1.0, forward_bar_type=MES=2021Z.SIM-1-DAY-MID-EXTERNAL, carry_price=1.0, carry_bar_type=MES=2021Z.SIM-1-DAY-MID-EXTERNAL, ts_event=0, ts_init=0)"  # noqa
        )

    def test_to_dict(self):
        # Arrange
        price = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=Price.from_str("1.0"),
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=Price.from_str("1.0"),
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        values = MultiplePrice.to_dict(price)

        # Assert
        assert values == {
            "bar_type": "MES.SIM-1-DAY-MID-EXTERNAL",
            "current_price": "1.1",
            "current_bar_type": "MES=2021X.SIM-1-DAY-MID-EXTERNAL",
            "forward_price": "1.0",
            "forward_bar_type": "MES=2021Z.SIM-1-DAY-MID-EXTERNAL",
            "carry_price": "1.0",
            "carry_bar_type": "MES=2021Z.SIM-1-DAY-MID-EXTERNAL",
            "ts_event": 0,
            "ts_init": 0,
        }

    def test_from_dict_returns_expected_price(self):
        # Arrange
        price = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=Price.from_str("1.0"),
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=Price.from_str("1.0"),
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        result = MultiplePrice.from_dict(MultiplePrice.to_dict(price))

        # Assert
        assert result == price

    def test_pickle_bar(self):
        # Arrange
        price = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=None,
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=None,
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        pickled = pickle.dumps(price)
        unpickled = pickle.loads(pickled)  # noqa S301 (pickle is safe here)

        # Assert
        assert unpickled == price

    def test_continuous_price_serialize_roundtrip(self):
        
        # Arrange
        register_arrow(
            data_cls=MultiplePrice,
            schema=MultiplePrice.schema(),
            serializer=make_dict_serializer(schema=MultiplePrice.schema()),
            deserializer=make_dict_deserializer(data_cls=MultiplePrice),
        )

        price = MultiplePrice(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            current_price=Price.from_str("1.1"),
            current_bar_type=BarType.from_str("MES=2021X.SIM-1-DAY-MID-EXTERNAL"),
            forward_price=None,
            forward_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            carry_price=None,
            carry_bar_type=BarType.from_str("MES=2021Z.SIM-1-DAY-MID-EXTERNAL"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        serialized = ArrowSerializer.serialize(price, data_cls=MultiplePrice)
        deserialized = ArrowSerializer.deserialize(data_cls=MultiplePrice, batch=serialized)

        # Assert
        assert deserialized[0] == price
