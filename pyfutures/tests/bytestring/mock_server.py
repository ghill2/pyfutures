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
from pyfutures.client.protocol import parse_buffer
import traceback


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
#
# You cannot combine utf-8 encoded bytestrings and bytestring saved from gateway into a bytestring
# and send it to stdout to be logged by the main process


bytestring_dir = PACKAGE_ROOT / "tests" / "bytestring" / "txt"

STDOUT = None


def stdout(buf):
    global STDOUT
    STDOUT.write(buf + "\n")


class BytestreamReplay:
    def __init__(self, data):
        self._bstream = data
        self._bpos = 0

    # def next_write_index(self):
    #     """
    #     set bpos to the next write index
    #     """
    #
    #     raise ValueError("No more writes encountered...")
    #
    def send_responses(self, writer):
        """
        sends all READs from the current WRITE to the next WRITE
        """
        pos = self._bpos
        while pos < len(self._bstream):
            pos = pos + 1
            dir = self._bstream[pos][0]
            buf = self._bstream[pos][1]
            if dir == "WRITE":
                continue
            writer.write(buf)

        self._bpos = pos
        return


class IO:
    def __init__(self) -> None:
        self._breplay: BytestreamReplay | None = None
        self._is_connected = asyncio.Event()

    async def read_stdin_task(self):
        try:
            while True:
                await asyncio.sleep(0)
                data = await self.stdin.read(4096)
                if not data:
                    continue
                stdout(b"mock_server stdin: " + data)
                path = Path(data.decode("utf-8"))
                # self.stdout.write("==== Loaded Bytestrings: ====\n".encode("utf-8"))
                # for line in self._bstream:
                #     self.stdout.write(
                #         f"{line[0]} ".encode("utf-8") + line[1] + "\n".encode("utf-8")
                #     )
                self._breplay = BytestreamReplay(data=pickle.load(open(path, "rb")))
                self._is_connected.clear()
                stdout(b"BYTESTRINGS READY")
        except Exception:
            print(traceback.print_exc())
            exit()

    async def setup_stdin_stdout(self):
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        w_transport, w_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout
        )
        global STDOUT
        STDOUT = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
        self.stdin = reader
        return reader

    async def handle_client(self, reader, writer):
        try:
            while True:
                await asyncio.sleep(0)
                data = await reader.read(256)
                if not data:
                    continue
                stdout(b"mock_server read: " + data + b"\n")

                if self._is_connected.is_set():
                    self._breplay.send_responses(writer)
                else:
                    buf = data
                    if buf.startswith(b"API\x00\x00\x00\x00\tv176..176"):
                        self._breplay.send_responses(writer)
                        buf = buf[0:29]

                    if buf.startswith(b"\x00\x00\x00") and b"71\x002\x00" in buf:
                        self._breplay.send_responses(writer)

                # for buf in bufs:
                # self.stdout.write(b"mock_server read: " + data + b"\n")

                # TODO: get this working
                # assert (
                #     self._bstream[pos][1] == data
                # ), f"{self._bstream[pos][1]} != {data}"

                # while pos < len(self._bstream):
                #     if self._bstream[pos][0] == "WRITE":
                #         break
                #     response = self._bstream[pos][1]
                #     # self.stdout.write(
                #     #     "mock_server write: ".encode("utf-8")
                #     #     + repr(response).
                #     #     + "\n".encode("utf-8")
                #     # )
                #     writer.write(response)
                #     pos = pos + 1
        except Exception:
            print(traceback.print_exc())
            exit()


async def main():
    io = IO()
    await io.setup_stdin_stdout()
    stdout("Subprocess Started...\n")
    port = 8890

    asyncio.create_task(io.read_stdin_task(), name="mock_server read_stdin_task")

    server = await asyncio.start_server(
        io.handle_client,
        "localhost",
        port,
        reuse_port=True,
    )
    stdout(b"mock_server started on port {port}...")
    stdout(b"MOCK_SERVER READY")
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
