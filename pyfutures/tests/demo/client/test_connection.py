import asyncio
import logging
import sys

import pandas as pd
import pytest
from ibapi.contract import Contract as IBContract

# from nautilus_trader.adapters.interactive_brokers.gateway import (
#     InteractiveBrokersGateway,
# )
# from nautilus_trader.adapters.interactive_brokers.config import (
#     InteractiveBrokersGatewayConfig,
# )
#
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import Frequency
from pyfutures.client.enums import WhatToShow


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from pyfutures.tests.demo.client.gateway import Gateway
from pyfutures.tests.demo.client.stubs import ClientStubs


# TO SHOW LOGS / OUTPUT:
# pytest -o log_cli=true
#
# Requirements: ensure Docker Desktop is running on OSX
# or get docker CLI version working by manually running the docker daemon.


# @pytest.fixture(scope="session")
# def gateway():
#     return InteractiveBrokersGateway(
#         trading_mode="paper",
#         config=InteractiveBrokersGatewayConfig(
#             username=dotenv_values()["TWS_USERNAME"],
#             password=dotenv_values()["TWS_PASSWORD"],
#         ),
#     )

###########
## TEST CASE 1:
# A request is made after the empty bytestring,
# before the client has reconnected again
# this can be before the watchdog has attempted to reconnected, or during
## TEST CASE 2:
# A request is made before the empty bytestring
# while waiting for a response, the empty bytestring is received
#

# TODO: ALways start the container once before running all tests
# Sometimes the docker container socat responds with Connection Refused...


@pytest.mark.asyncio()
async def test_connect(event_loop):
    print("test_connect")
    connection = ClientStubs.connection(loop=event_loop, client_id=1)
    await connection.connect(timeout_seconds=20)


@pytest.fixture(scope="session")
def gateway():
    gateway = Gateway(log_level=logging.DEBUG)
    return gateway


@pytest.mark.asyncio()
async def test_reconnect(gateway, event_loop):
    """
    Tests if the watch dog reconnects
    This also tests reconnect on empty bytestring
    as an empty bytestring is sent to the pyfutures client when the docker container shuts down
    """
    client = ClientStubs.client(loop=event_loop)
    await gateway.start()
    await client.connect()
    expected = await client.request_account_summary()
    print(expected)
    # await gateway.restart()
    # At this point, gateway has restarted
    # in the worst case,
    # there needs to be a wait of 5 seconds for an entire watchdog task cycle
    # and an additional ~5 seconds to allow for the reconnection
    # this is only temporarily required as the client currently
    # does not have any handling for requests that get sent when the client is disconnected
    print("Waiting 10 seconds ")
    await asyncio.sleep(10)
    details = await client.request_account_summary()
    assert details == expected
    print(details)


# Possibly test all methods in these 2 tests to avoid the amount of docker container restarts
@pytest.mark.asyncio()
async def test_disconnect_then_request(gateway, event_loop):
    """
    If request_bars() is executed when the client is disconnected
    the client should wait to send the request until the client is connected again
    """
    client = ClientStubs.client(loop=event_loop)
    contract = IBContract()
    contract.secType = "CONTFUT"
    contract.exchange = "CME"
    contract.symbol = "DA"

    # start the test connected
    await gateway.start()
    await client.connect()
    await client.request_account_summary()  # is_connected

    # stop gateway (simulate disconnection)
    await gateway.stop()
    # start gateway again, do not block
    asyncio.create_task(gateway.start())
    # try to send a client request when the gateway is disconnected
    bars = await client.request_bars(
        contract=contract,
        bar_size=BarSize._1_DAY,
        what_to_show=WhatToShow.TRADES,
        duration=Duration(step=7, freq=Frequency.DAY),
        end_time=pd.Timestamp.utcnow() - pd.Timedelta(days=1).floor("1D"),
    )
    print("BARS")
    print(type(bars))
    print(bars)


@pytest.mark.asyncio()
async def test_request_then_disconnect(event_loop):
    """
    if request_bars() is executed and then the client is disconnected before a response is received
    the client should immediately set the response to a ClientDisconnected Exception so the parent can handle?
    """
    client = ClientStubs.client(loop=event_loop)
