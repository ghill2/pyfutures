import asyncio
import socket
from pyfutures import PACKAGE_ROOT
import sys
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
import functools
from pathlib import Path
import pickle


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

class ByteReplayer:
    def __init__(self, bstream: tuple(str, bytes)):
        self._bstream = bstream
        self._bpos = 0

    def find_start():

    def send_responses():
        """
            send READ bytes from current WRITE pos to next WRITE pos
        """

class IO:
    _bstream = []
    _bplayer = ByteReplayer | None = None

    async def read_stdin_task(self):
        while True:
            await asyncio.sleep(0)
            data = await self.stdin.read(256)
            if not data:
                continue
            self.stdout.write("mock_server stdin: ".encode("utf-8") + data)
            self.stdout.write("\n".encode("utf-8"))
            path = Path(data)
            self._bstream = pickle.load(open(path, "rb"))
            self._bpos = 0
            self.stdout.write("BYTESTRINGS READY\n")

    async def setup_stdin_stdout(self):
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        w_transport, w_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout
        )
        self.stdout = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
        self.stdin = reader
        return self.stdout

    async def handle_client(self, reader, writer, stdout):
        while True:
            await asyncio.sleep(0)
            data = await reader.read(1024)
            if not data:
                continue
            self.stdout.write("mock_server read: ".encode("utf-8") + data)
            self.stdout.write("\n".encode("utf-8"))

            pos = self._bpos
            assert self._bstream[pos] == data
            pos = pos + 1
            while True:
                if self._bstream[pos][0] == "WRITE":
                    break
                writer.write(self._bstream[pos])
            self._bpos = pos


async def main():
    io = IO()
    stdout = await io.setup_stdin_stdout()
    stdout.write("Subprocess Started...\n".encode("utf-8"))
    port = 8890

    asyncio.create_task(io.read_stdin_task(), name="mock_server read_stdin_task")

    server = await asyncio.start_server(
        io.handle_client,
        "localhost",
        port,
        reuse_port=True,
    )
    stdout.write(f"mock_server started on port {port}...\n".encode("utf-8"))
    stdout.write(f"MOCK_SERVER READY\n".encode("utf-8"))
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
