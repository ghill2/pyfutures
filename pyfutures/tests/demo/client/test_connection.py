import asyncio
import logging
import sys

from pyfutures.tests.test_kit import IBTestProviderStubs

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

from pyfutures.logger import LoggerAttributes


# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from pyfutures.tests.demo.client.gateway import Gateway
from pyfutures.tests.demo.client.stubs import ClientStubs


# from pyfutures.adapter.enums import WhatToShow

# Requirements: ensure Docker Desktop is running on OSX
# or get docker CLI version working by manually running the docker daemon.

###########
## TEST CASE 1:
# A request is made after the empty bytestring,
# before the client has reconnected again
# this can be before the watchdog has attempted to reconnected, or during
## TEST CASE 2:
# A request is made before the empty bytestring
# while waiting for a response, the empty bytestring is received
#

# TODO: If building into CI / make test workflow: aLways restart the container once before running all tests
# Sometimes the docker container socat responds with Connection Refused...
# If this doesnt work reliably, try removing the container and creating it again

# with IB Eclient/EWrapper:
# > clientId=10 -> NullPointerException, no OutofMemoryError
#
#

from pyfutures.logger import LoggerAdapter

_log = LoggerAdapter.from_name(name="test_connection.py")


@pytest.mark.asyncio()
async def test_connect(event_loop):
    event_loop.set_debug(True)
    LoggerAttributes.level = logging.DEBUG
    client = ClientStubs.client(loop=event_loop)
    await client.connect(client_id=10)
    await client.request_account_summary()
    # await asyncio.sleep(20)


@pytest.mark.asyncio()
async def test_multiple_client_id(event_loop):
    for i in range(30, 40):
        print("=========================================")
        client = ClientStubs.uncached_client(client_id=i, loop=event_loop)
        await client.connect()
        # await asyncio.sleep(5)


@pytest.fixture(scope="session")
def gateway():
    gateway = Gateway(log_level=logging.DEBUG)
    return gateway


@pytest.mark.timeout(240)
@pytest.mark.asyncio()
async def test_reconnect_subscriptions(gateway, event_loop):
    """
    tests if subscriptions are reconnected when the client receives an empty bytestring
    tests if the client reconnects if the gateway connection is dropped
    """

    client = ClientStubs.client(client_id=10, loop=event_loop)

    await gateway.start()
    await client.connect(timeout_seconds=20)

    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "CONTFUT"

    bars = []
    client.subscribe_bars(
        contract=contract,
        what_to_show=WhatToShow.TRADES,
        bar_size=BarSize._5_SECOND,
        callback=lambda b: bars.append(b),
    )

    # wait until the first bar is collected
    while len(bars) == 0:
        await asyncio.sleep(0.1)

    await gateway.restart()

    # expected = await client.request_account_summary()
    # print(expected)
    # At this point, gateway has restarted
    # in the worst case,
    # there needs to be a wait of 5 seconds for an entire watchdog task cycle
    # and an additional ~5 seconds to allow for the reconnection
    # this is only temporarily required as the client currently
    # does not have any handling for requests that get sent when the client is disconnected
    #
    _log.debug("Waiting until client is reconnected...")
    await client.connection._is_connected.wait()
    _log.debug("Successfully reconnected...")
    _log.debug(f"len(bars): {len(bars)}")

    # wait until the first bar after collection is received
    while len(bars) < 2:
        await asyncio.sleep(0.1)

    assert len(bars) == 2

    # details = await client.request_account_summary()
    # print(details)
    # assert details.account == expected.account
    # await asyncio.sleep(20)


# @pytest.mark.skip(reason="helper")
def test_helper_ibapi_connection():
    """
    Helper test to reference / compare ibapi connection bytestrings with pyfutures connection bytestrings


    Bytestrings that worked when clientId > 10:
        handshake:
            msg = b"API\x00\x00\x00\x00\tv100..176"
        startApi:
            msg = b"\x00\x00\x00\t71\x002\x0012\x00\x00"
    """
    from ibapi.wrapper import EWrapper
    from ibapi.client import EClient
    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    eclient = EClient(wrapper=EWrapper())

    eclient.clientId = 1
    names = logging.Logger.manager.loggerDict
    for name in names:
        if "ibapi" in name:
            logging.getLogger(name).setLevel(logging.DEBUG)

    eclient.connect("127.0.0.1", 4002, 1)


@pytest.mark.skip(reason="helper")
def test_nautilus_gateway():
    """
    Used to generate the docker API configuration to use in aiodocker
    log the config in python (sync) docker package
    """
    from dotenv import dotenv_values

    from nautilus_trader.adapters.interactive_brokers.config import (
        InteractiveBrokersGatewayConfig,
    )
    from nautilus_trader.adapters.interactive_brokers.gateway import (
        InteractiveBrokersGateway,
    )

    config = InteractiveBrokersGatewayConfig(
        start=False,
        username=dotenv_values()["TWS_USERNAME"],
        password=dotenv_values()["TWS_PASSWORD"],
        trading_mode="paper",
        read_only_api=True,
    )

    gateway = InteractiveBrokersGateway(config=config)
    gateway.start()


# Possibly test all methods in these 2 tests to avoid the amount of docker container restarts
# @pytest.mark.asyncio()
# async def test_disconnect_then_request(gateway, event_loop):
#     """
#     If request_bars() is executed when the client is disconnected
#     the client should wait to send the request until the client is connected again
#     """
#     client = ClientStubs.client(loop=event_loop)
#     contract = IBContract()
#     contract.secType = "CONTFUT"
#     contract.exchange = "CME"
#     contract.symbol = "DA"
#
#     # start the test connected
#     await gateway.start()
#     await client.connect()
#     await client.request_account_summary()  # is_connected
#
#     # stop gateway (simulate disconnection)
#     await gateway.stop()
#     # start gateway again, do not block
#     asyncio.create_task(gateway.start())
#     # try to send a client request when the gateway is disconnected
#     bars = await client.request_bars(
#         contract=contract,
#         bar_size=BarSize._1_DAY,
#         what_to_show=WhatToShow.TRADES,
#         duration=Duration(step=7, freq=Frequency.DAY),
#         end_time=pd.Timestamp.utcnow() - pd.Timedelta(days=1).floor("1D"),
#     )
#     print("BARS")
#     print(type(bars))
#     print(bars)


# @pytest.mark.asyncio()
# async def test_request_then_disconnect(event_loop):
#     """
#     if request_bars() is executed and then the client is disconnected before a response is received
#     the client should immediately set the response to a ClientDisconnected Exception so the parent can handle?
#     """
#     client = ClientStubs.client(loop=event_loop)
