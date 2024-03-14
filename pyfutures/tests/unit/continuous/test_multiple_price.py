import pickle

from nautilus_trader.continuous.contract_month import ContractMonth
from nautilus_trader.continuous.price import MultiplePrice
from nautilus_trader.model.objects import Price
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.test_kit.stubs.data import TestDataStubs


class TestMultiplePrice:
    def test_continuous_price_equality(self):
        # Arrange
        price1 = MultiplePrice(
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
            ts_event=0,
            ts_init=0,
        )
        price2 = MultiplePrice(
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            current_price=None,
            current_month=ContractMonth("2021X"),
            carry_price=None,
            carry_month=ContractMonth("2021Z"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert price1 == price1
        assert price2 == price2

    def test_continuous_price_str_and_repr(self):
        # Arrange
        price = MultiplePrice(
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert (
            str(price)
            == "MultiplePrice(bar_type=AUD/USD.SIM-1-MINUTE-BID-EXTERNAL, current_price=1.1, current_month=2021X, forward_price=1.0, forward_month=2021Z, carry_price=1.0, carry_month=2021Z, ts_event=0, ts_init=0)"  # noqa
        )
        assert (
            repr(price)
            == "MultiplePrice(bar_type=AUD/USD.SIM-1-MINUTE-BID-EXTERNAL, current_price=1.1, current_month=2021X, forward_price=1.0, forward_month=2021Z, carry_price=1.0, carry_month=2021Z, ts_event=0, ts_init=0)"  # noqa
        )

    def test_to_dict(self):
        # Arrange
        price = MultiplePrice(
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        values = MultiplePrice.to_dict(price)

        # Assert
        assert values == {
            "bar_type": "AUD/USD.SIM-1-MINUTE-BID-EXTERNAL",
            "current_price": "1.1",
            "current_month": "2021X",
            "forward_price": "1.0",
            "forward_month": "2021Z",
            "carry_price": "1.0",
            "carry_month": "2021Z",
            "ts_event": 0,
            "ts_init": 0,
        }

    def test_from_dict_returns_expected_price(self):
        # Arrange
        price = MultiplePrice(
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
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
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
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
            bar_type=TestDataStubs.bartype_audusd_1min_bid(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("2021X"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("2021Z"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("2021Z"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        serialized = ArrowSerializer.serialize(price, data_cls=MultiplePrice)
        deserialized = ArrowSerializer.deserialize(data_cls=MultiplePrice, batch=serialized)

        # Assert
        assert deserialized[0] == price
