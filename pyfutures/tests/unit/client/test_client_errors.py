from pyfutures.client.client import InteractiveBrokersClient
import pytest
from unittest.mock import Mock
from pyfutures.tests.unit.client.stubs import ClientStubs

from pyfutures.tests.unit.client.mock_server import MockServer


# PROBLEMS:
# no exceptions are displayed in the _listen_task
# any exceptions encountered will cancel the _listen_task, making the client unresponsive
# any exceptions encountered will not send back an exception to the request object
# the request happens, then exception is encountered in the callback
# we can log a message, but the client is still waiting for a timeout, because the request is still awaiting
#
# possibly expand this test to cover all methods on the client
#


@pytest.mark.asyncio()
async def test_handle_exception_in_callback(mocker, event_loop):
    """
    if an exception is encountered in the callbacks
      - the client should display the exception in the logs
      - the client should send back a ClientException in the reqId ? or wait until timeout
      - the _listen_task should not cancel
    """
    client = ClientStubs.client(loop=event_loop)
    mock_server = MockServer()
    mocker.patch(
        "asyncio.open_connection",
        return_value=(mock_server.reader, mock_server.writer),
    )
    await client.connect(timeout_seconds=0.1)
    assert client._connection.is_connected

    accounts_res = b"\x00\x00\x00\x0f15\x001\x00DU1234567\x00"
    mock_server.queue_response(msg=accounts_res)

    client.managedAccounts = Mock(side_effect=Exception)
    # this will receive a timeouterror if there is no data
    # AND if there is but the callback raised an exception
    accounts_exception = await client.request_accounts()
    client.managedAccounts = InteractiveBrokersClient.managedAccounts
    accounts = await client.request_accounts()
    print(accounts)
