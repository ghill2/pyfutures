import pickle

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.model.objects import Price
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


class TestMultiplePrice:
    def test_continuous_price_equality(self):
        # Arrange
        price1 = MultiplePrice(
            instrument_id=TestIdStubs.gbpusd_id(),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
            ts_event=0,
            ts_init=0,
        )
        price2 = MultiplePrice(
            instrument_id=TestIdStubs.gbpusd_id(),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            current_price=None,
            current_month=ContractMonth("X21"),
            carry_price=None,
            carry_month=ContractMonth("Z21"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert price1 == price1
        assert price2 == price2

    def test_continuous_price_str_and_repr(self):
        # Arrange
        price = MultiplePrice(
            instrument_id=TestIdStubs.gbpusd_id(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
            ts_event=0,
            ts_init=0,
        )

        # Act, Assert
        assert (
            str(price)
            == "MultiplePrice(instrument_id=GBP/USD.SIM, current_price=1.1, current_month=X21, forward_price=1.0, forward_month=Z21, carry_price=1.0, carry_month=Z21, ts_event=0, ts_init=0)"  # noqa
        )
        assert (
            repr(price)
            == "MultiplePrice(instrument_id=GBP/USD.SIM, current_price=1.1, current_month=X21, forward_price=1.0, forward_month=Z21, carry_price=1.0, carry_month=Z21, ts_event=0, ts_init=0)"  # noqa
        )

    def test_to_dict(self):
        # Arrange
        price = MultiplePrice(
            instrument_id=TestIdStubs.gbpusd_id(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        values = MultiplePrice.to_dict(price)

        # Assert
        assert values == {
            "instrument_id": "GBP/USD.SIM",
            "current_price": "1.1",
            "current_month": "X21",
            "forward_price": "1.0",
            "forward_month": "Z21",
            "carry_price": "1.0",
            "carry_month": "Z21",
            "ts_event": 0,
            "ts_init": 0,
        }

    def test_from_dict_returns_expected_price(self):
        # Arrange
        price = MultiplePrice(
            instrument_id=TestIdStubs.gbpusd_id(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
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
            instrument_id=TestIdStubs.gbpusd_id(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
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
            instrument_id=TestIdStubs.gbpusd_id(),
            current_price=Price.from_str("1.1"),
            current_month=ContractMonth("X21"),
            forward_price=Price.from_str("1.0"),
            forward_month=ContractMonth("Z21"),
            carry_price=Price.from_str("1.0"),
            carry_month=ContractMonth("Z21"),
            ts_event=0,
            ts_init=0,
        )

        # Act
        serialized = ArrowSerializer.serialize(price, data_cls=MultiplePrice)
        deserialized = ArrowSerializer.deserialize(data_cls=MultiplePrice, batch=serialized)

        # Assert
        assert deserialized[0] == price
