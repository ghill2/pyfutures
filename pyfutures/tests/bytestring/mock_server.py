import asyncio
import socket
from pyfutures import PACKAGE_ROOT


class MockServer:
    gateway_port = 8888
    comm_port = 9999

    bytestring_dir = PACKAGE_ROOT / "tests" / "bytestring" / "txt"

    def __init__(self):
        self.is_ready = asyncio.Event()

    async def _handle_stdin(self):
        while True:
            data = await asyncio.get_event_loop().run_in_executor(None, input, 2)
            await asyncio.sleep(0)

        print("HANDLE STDIN ENDING")

    async def _handle_client(self, reader, writer):
        """
        Gateway server simulation
        """
        while True:
            data = await reader.read(1024)
            if not data:
                break
            print(f"GATEWAY: {data.decode()}")
            # writer.write(message.encode())
            await writer.drain()
            await asyncio.sleep(0)

    async def start(self):
        stdin = asyncio.create_task(self._handle_stdin())
        server = asyncio.start_server(self._handle_client, "localhost", self.comm_port)
        # comm_server = asyncio.start_server(self._handle_comm, "localhost", self.gateway_port)

        # tasks = [server.serve_forever(), comm_server.serve_forever()]

        async with server, stdin:
            print(f"Serving on ports: {self.gateway_port} and {self.comm_port}")
            await asyncio.gather(*[server.serve_forever(), stdin])

    # async def load_bytestrings():


async def main():
    mock_server = MockServer()
    await mock_server.start()


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
