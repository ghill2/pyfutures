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
    STDOUT.write(buf + b"\n")


class BytestreamReplay:
    def __init__(self, data):
        self._data = data
        self._bpos = 0

    def send_responses(self, writer):
        """
        Sends all response / read bufs for the associated write
        """
        req_res = self._data[self._bpos]
        for res in req_res[1]:
            writer.write(res)
        self._bpos = self._bpos + 1
        # len_responses_b = str(len(responses)).encode("utf-8")
        # stdout(b"Sending " + len_responses_b + b" for this WRITE: " + req_res[0])


class IO:
    def __init__(self) -> None:
        self._breplay: BytestreamReplay | None = None
        self._handshake_recv = asyncio.Event()
        self._startapi_recv = asyncio.Event()
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
                #
                bytestream = pickle.load(open(path, "rb"))
                len_bytestream_b = str(len(bytestream)).encode("utf-8")
                stdout(b"Loaded " + len_bytestream_b + b" requests to replay...")
                self._breplay = BytestreamReplay(data=bytestream)
                self._handshake_recv.clear()
                self._startapi_recv.clear()
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
                data = await reader.read(4096)
                if not data:
                    continue
                stdout(b"recv: " + data)

                if self._is_connected.is_set():
                    self._breplay.send_responses(writer)
                else:
                    buf = data
                    handshake_bytes = b"API\x00\x00\x00\x00\tv176..176"
                    if buf.startswith(handshake_bytes):
                        self._breplay.send_responses(writer)
                        buf = buf.removeprefix(handshake_bytes)
                        self._handshake_recv.set()
                    # stdout(b"remaining buf for start API is: " + buf)
                    if buf.startswith(b"\x00\x00\x00") and b"71\x002\x00" in buf:
                        self._breplay.send_responses(writer)
                        self._startapi_recv.set()

                    if self._handshake_recv.is_set() and self._startapi_recv.is_set():
                        self._is_connected.set()

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
    stdout(b"Subprocess Started...")
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
    #
    #
    #
    #    def send_responses(self, writer):
    # """
    # sends all READs from the current WRITE to the next WRITE
    # """
    # pos = self._bpos
    # cbuf_w = self._bstream[pos][1]
    # responses = []
    # while pos < len(self._bstream):
    #     pos = pos + 1
    #     dir = self._bstream[pos][0]
    #     buf = self._bstream[pos][1]
    #     if dir == "WRITE":
    #         break
    #     responses.append(buf)
    #
    # self._bpos = pos
    # len_responses_b = str(len(responses)).encode("utf-8")
    # stdout(b"Sending " + len_responses_b + b" for this WRITE: " + cbuf_w)
    # for buf in responses:
    #     writer.write(buf)
    # return
