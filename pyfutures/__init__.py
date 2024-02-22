from pathlib import Path
import os

PACKAGE_ROOT = Path(os.path.dirname(os.path.abspath(__file__)))

from dotenv import dotenv_values

PROXY_EMAIL = dotenv_values().get("PROXY_EMAIL")
PROXY_PASSWORD = dotenv_values().get("PROXY_PASSWORD")
IB_ACCOUNT_ID = dotenv_values().get("IB_ACCOUNT_ID")
IB_USERNAME = dotenv_values().get("IB_USERNAME")
IB_PASSWORD = dotenv_values().get("IB_PASSWORD")

from nautilus_trader.serialization.arrow.serializer import make_dict_deserializer
from nautilus_trader.serialization.arrow.serializer import make_dict_serializer
from nautilus_trader.serialization.arrow.serializer import register_arrow
from pyfutures.continuous.multiple_bar import MultipleBar

register_arrow(
    data_cls=MultipleBar,
    schema=MultipleBar.schema(),
    encoder=make_dict_serializer(schema=MultipleBar.schema()),
    decoder=make_dict_deserializer(data_cls=MultipleBar),
)
