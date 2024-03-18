import asyncio
import queue


class MockSocket:
    def __init__(self, queue):
        self.message_queue = queue

    async def listen(self, port):
        server = await asyncio.start_server(self.handle_connection, "localhost", port)
        async with server:
            await asyncio.Future()  # Run server forever

    async def handle_connection(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            message = data.decode()
            reply = self.message_queue.get()  # Block until a reply is available
            writer.write(reply.encode())
            await writer.drain()


async def main(queue, port):
    mock_socket = MockSocket(queue)
    await mock_socket.listen(port)


if __name__ == "__main__":
    message_queue = queue.Queue()  # Thread-safe queue for message exchange
    asyncio.run(main(message_queue, 12345))  # Replace 12345 with your desired port
