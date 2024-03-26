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
import json
from ibapi import comm


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
def list_to_ascii_buf(data):
    buf = b"["
    for item in data:
        buf = buf + item.encode("ascii") + b", "
    buf = buf + b"]"
    return buf


bytestring_dir = PACKAGE_ROOT / "tests" / "bytestring" / "txt"

STDOUT = None


def stdout(buf):
    global STDOUT
    STDOUT.write(buf + b"\n")


class BytestreamReplay:
    def __init__(self, data, mock_server):
        self._data = data
        self._mock_server = mock_server
        self._bpos = 0

    def current(self):
        return self._data[self._bpos]

    async def send_responses(self, writer):
        """
        Sends all response / read bufs for the associated write
        """
        req_res = self._data[self._bpos]
        for res_fields in req_res[1]:
            # for field in res_fields:
            # stdout(field.encode("ascii"))
            if res_fields[0] == "eof":
                server = self._mock_server.server
                server.close()
                writer.write_eof()
                await server.wait_closed()
            else:
                # add null separators
                msg = ""
                for field in res_fields:
                    msg = msg + comm.make_field(field)
                msg = comm.make_msg(msg)  # add struct prefix
                writer.write(msg)
        self._bpos = self._bpos + 1


class MockServer:
    def __init__(self) -> None:
        self._breplay: BytestreamReplay | None = None

        self._handshake_recv = asyncio.Event()
        self._startapi_recv = asyncio.Event()
        # when handshake+startapi have been received
        self._is_connected = asyncio.Event()

        self.server = None

    def reset(self):
        self._handshake_recv.clear()
        self._startapi_recv.clear()
        self._is_connected.clear()

    async def restart_serve_forever(self):
        """
        Creates and serves the forever until
        A coroutine initiates a server restart
        """
        stdout(b"(re)Starting subproc Server...")
        self.reset()

        #
        stdout(b"mock_server started on port 8890")
        self.server = await asyncio.start_server(
            self.handle_client, "localhost", 8890, reuse_port=True, start_serving=False
        )
        await asyncio.sleep(1)

        try:
            await self.server.serve_forever()
        except asyncio.CancelledError:
            await self.restart_serve_forever()

    async def handle_client(self, reader, writer):
        """
        Test if this handles buffers with multiple messages
        """
        try:
            while True:
                await asyncio.sleep(0)
                data = await reader.read(4096)
                if not data:
                    continue

                if self._is_connected.is_set():
                    bufs = parse_buffer(data)
                    for buf in bufs:
                        _breplay_current = self._breplay.current()[0]

                        # convert the buf to string fields
                        fields = buf.split(b"\0")
                        ascii_fields = [f.decode("ascii") for f in fields]

                        stdout(
                            b"recv: " + list_to_ascii_buf(ascii_fields),
                        )
                        # log the comparison for debugging purposes
                        stdout(
                            str(self._breplay._bpos).encode("ascii")
                            + b"breplay: "
                            + list_to_ascii_buf(_breplay_current)
                        )

                        assert ascii_fields == _breplay_current

                        await self._breplay.send_responses(writer)
                else:
                    buf = data

                    stdout(str(self._breplay._bpos).encode("ascii") + b"recv: " + data)

                    handshake_bytes = b"API\x00\x00\x00\x00\tv176..176"
                    if buf.startswith(handshake_bytes):
                        assert self._breplay.current()[0] == ["handshake"]
                        await self._breplay.send_responses(writer)
                        buf = buf.removeprefix(handshake_bytes)
                        self._handshake_recv.set()
                    # stdout(b"remaining buf for start API is: " + buf)
                    if buf.startswith(b"\x00\x00\x00") and b"71\x002\x00" in buf:
                        assert self._breplay.current()[0] == ["startapi"]
                        await self._breplay.send_responses(writer)
                        self._startapi_recv.set()

                    if self._handshake_recv.is_set() and self._startapi_recv.is_set():
                        self._is_connected.set()
        except Exception:
            print(traceback.print_exc())
            exit()


async def load_bytestrings(path: Path, mock_server):
    mock_server.reset()
    path = Path(path)
    with open(path) as file:
        bytestream = json.load(file)
    len_bytestream_b = str(len(bytestream)).encode("utf-8")
    stdout(b"Loaded " + len_bytestream_b + b" requests to replay...")
    mock_server._breplay = BytestreamReplay(data=bytestream, mock_server=mock_server)

    # subprocess started but server has not been instantiated on the first read
    while mock_server.server is None:
        await asyncio.sleep(0)

    # wait until server is accepting new connections
    while not mock_server.server.is_serving():
        await asyncio.sleep(0)


async def read_stdin_task(reader, mock_server):
    try:
        while True:
            await asyncio.sleep(0)
            buf = await reader.read(4096)
            if not buf:
                continue
            stdout(b"mock_server stdin: " + buf)
            cmd, arg = tuple(buf.split(b"\x00"))
            cmd = cmd.decode("ascii")
            if arg is not None:
                arg = arg.decode("ascii")
            if cmd == "load_bytestrings":
                await load_bytestrings(path=arg, mock_server=mock_server)
            elif cmd == "reset":
                mock_server.reset()

            stdout(b"COMMAND SUCCESS")
    except Exception:
        print(traceback.print_exc())
        exit()


async def main():
    # setup stdin stdout
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    global STDOUT
    STDOUT = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)

    # instantiate server and serve forever
    mock_server = MockServer()

    # start task to read and react from stdin
    asyncio.create_task(
        read_stdin_task(reader, mock_server), name="mock_server read_stdin_task"
    )

    await mock_server.restart_serve_forever()


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
