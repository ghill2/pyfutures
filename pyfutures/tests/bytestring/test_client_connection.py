import asyncio
import logging

import pytest

from pyfutures.logger import LoggerAttributes
from pyfutures.tests.bytestring.stubs import BytestringClientStubs


#
# """
#     Problems with
#     Asyncio
# """
#
# def mocked_make_socket_transport():
#     """
#     This function returns _SelectorSocketTransport to the Protocol
#     patch it to return a Test Transport that reads bytestrings from the given path
#     and sends
#     """
#
# from pyfutures.client.protocol import Protocol
# class BytestringProtocol(Protocol):
#     def __init__():
#         pass
#
#
#     def write():
#         super().write():
#


# async def mock_create_connection(loop, protocol, host, port):
#     """
#     Returns _UnixSubprocessTransport instead of the _SelectorSocketTransport
#     """
#     print("MOCK CALLED")
#     filepath = PACKAGE_ROOT / "tests" / "bytestring" / "mock_server.py"
#     protocol._pipe_data_received = lambda self, fd, data: self.data_received(data))
#     transport = await loop._make_subprocess_transport(
#         protocol,  # protocol
#         # f"{sys.executable} {filepath}",  # popen_args
#         [sys.executable, filepath],  # popen_args
#         False,  # shell
#         subprocess.PIPE,  # stdin
#         subprocess.PIPE,  # stdout
#         subprocess.PIPE,  # stderr
#         0,  # bufsize
#     )


@pytest.mark.asyncio()
async def test_connect(event_loop, mode):
    client = await BytestringClientStubs(mode=mode, loop=event_loop).client(
        loop=event_loop
    )
    LoggerAttributes.level = logging.DEBUG
    await client.connect()
    # await client.request_account_summary()
    print("asyncio sleeping...")
    await asyncio.sleep(1000)

    # to tear down, send an EOF
