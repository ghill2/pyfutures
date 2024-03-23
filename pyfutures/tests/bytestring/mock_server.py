import asyncio
import socket
from pyfutures import PACKAGE_ROOT
import sys
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
import functools


# def write_to_file(msg):
#     path = "/Users/f1/tmp/server.log"
#     with open(path, "w") as f:
#         f.write("HANDLE READ RECEIVED DATA")
#
#
# TOMORROW:
# get this loading a bytestring file . check if exists should be in main process
# call connect and save the bytestrings
# then handle the bytestrings on the server in unit test mode


bytestring_dir = PACKAGE_ROOT / "tests" / "bytestring" / "txt"


async def read_stdin_task(stdin, stdout):
    while True:
        await asyncio.sleep(0)
        data = await stdin.read(256)
        if not data:
            continue
        stdout.write("mock_server: Recv from main process: ".encode("utf-8") + data)


async def _handle_client(stdout):
    """
    Gateway server simulation
    """

    return handle_client


async def setup_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    stdout = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, stdout


async def handle_client(reader, writer, stdout):
    while True:
        await asyncio.sleep(0)
        data = await reader.read(1024)
        if not data:
            continue
        stdout.write("mock_server: Recv on gateway: ".encode("utf-8") + data)
        stdout.write("\n".encode("utf-8"))


async def main():
    stdin, stdout = await setup_stdin_stdout()
    stdout.write("Subprocess Started...\n".encode("utf-8"))
    port = 8890

    asyncio.create_task(
        read_stdin_task(stdin, stdout), name="mock_server read_stdin_task"
    )

    server = await asyncio.start_server(
        functools.partial(handle_client, stdout=stdout),
        "localhost",
        port,
        reuse_port=True,
    )
    stdout.write(f"mock_server started on port {port}...\n".encode("utf-8"))
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

    # async def _handle_comm(self, reader, writer):
    #     """Loads bytestrings"""
    #     while True:
    #         data = await reader.read(1024)
    #         if not data:
    #             break
    #         print(f"COMM_SERVER: {data.decode()}")
    #         await asyncio.sleep(0)
    #
