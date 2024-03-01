import pytest

from nautilus_trader.adapters.interactive_brokers.gateway import (
    InteractiveBrokersGateway,
)
from nautilus_trader.adapters.interactive_brokers.config import (
    InteractiveBrokersGatewayConfig,
)
from dotenv import dotenv_values
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


# TO SHOW LOGS / OUTPUT:
# pytest -o log_cli=true
#
# Requirements: ensure Docker Desktop is running on OSX
# or get docker CLI version working by manually running the docker daemon.


@pytest.fixture(scope="session")
def gateway():
    return InteractiveBrokersGateway(
        trading_mode="paper",
        config=InteractiveBrokersGatewayConfig(
            username=dotenv_values()["TWS_USERNAME"],
            password=dotenv_values()["TWS_PASSWORD"],
        ),
    )


@pytest.mark.asyncio()
async def test_handle_disconnect_on_empty_bytestring(connection):
    # check the handle disconnect method is called when a empty bytestring is received on the reader
    pass


@pytest.mark.asyncio()
async def test_reconnect(gateway, connection):
    gateway.start()
    await connection.connect()
