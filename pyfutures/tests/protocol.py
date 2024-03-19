import asyncio
from asyncio.streams import StreamReader
from asyncio.streams import StreamReaderProtocol
from asyncio.streams import StreamWriter
from asyncio.protocols import Protocol


class MyProtocol(Protocol):
    def connection_made(self, transport):
        # handshake
        print("connection made")

    def data_received(self, data):
        print("data received")
        print(data)

    def eof_received(self):
        print("eof received")

    def connection_lost(self, exc):
        # reconnect
        print("connection lost")
        print(exc)


async def main():
    host = "127.0.0.1"
    port = 4002
    _loop = asyncio.get_running_loop()
    _protocol = MyProtocol()
    _transport, _ = await _loop.create_connection(lambda: _protocol, host, port)
    _transport.write(b"API\x00\x00\x00\x00\tv176..176")
    await asyncio.sleep(20)


asyncio.run(main())
