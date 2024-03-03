import pytest

# from nautilus_trader.adapters.interactive_brokers.gateway import (
#     InteractiveBrokersGateway,
# )
# from nautilus_trader.adapters.interactive_brokers.config import (
#     InteractiveBrokersGatewayConfig,
# )
import logging
import sys
import asyncio

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from pyfutures.tests.client.demo.gateway import Gateway


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

@pytest.fixture(scope="session")
def gateway():
    gateway = Gateway(log_level=logging.DEBUG)
    return gateway







@pytest.mark.asyncio()
async def test_reconnect(gateway, client):
    """
        Tests if the watch dog reconnects
        This also tests reconnect on empty bytestring
        as an empty bytestring is sent to the pyfutures client when the docker container shuts down
    """
    await gateway.start()
    await client.connect()
    expected = await client.request_account_summary()
    print(expected)
    await gateway.restart()
    # At this point, gateway has restarted
    # in the worst case,
    # there needs to be a wait of 5 seconds for an entire watchdog task cycle
    # and an additional ~5 seconds to allow for the reconnection
    # this is only temporarily required as the client currently 
    # does not have any handling for requests that get sent when the client is disconnected
    print("Waiting 10 seconds ")
    await asyncio.sleep(10)
    details = await client.request_account_summary()
    print(details)
    
