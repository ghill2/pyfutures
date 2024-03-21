# MODIFIED FROM:
# https://github.com/jonathanslenders/asyncio-redis/blob/master/asyncio_redis/connection.py
import asyncio
from pyfutures.logger import LoggerAdapter
from typing import Callable
from pyfutures.client.protocol import Protocol


class Connection:
    """
    Wrapper around the protocol and transport which takes care of establishing
    the connection and reconnecting it.
    """

    def __init__(
        self,
        loop,
        subscriptions: dict,
        fields_received_callback: Callable,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
    ):
        self._loop = loop
        self._subscriptions = subscriptions
        self.host = host
        self.port = port

        self.reconnect_task: asyncio.Task | None = None

        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self.protocol = Protocol(
            loop=loop,
            client_id=client_id,
            connection_lost_callback=self._connection_lost_callback,
            fields_received_callback=fields_received_callback,
        )

        self._is_connected_waiter = None

    def _connection_lost_callback(self):
        print("connection lost callback")
        self.reconnect_task = self._loop.create_task(self._reconnect_task(), name="reconnect")

    async def _connect(self):
        self._log.info("Connecting...")
        await self.create_connection(self._loop, self.protocol, self.host, self.port)
        await self.protocol.perform_handshake()

        # reconnect subscriptions
        if len(self._subscriptions) > 0:
            self._log.debug(f"Reconnecting subscriptions {self._subscriptions=}")
            for sub in self._subscriptions.values():
                sub.subscribe()

        if self.reconnect_task is not None:
            self.reconnect_task.cancel()
            self._log.info("Reconnect task cancelled...")

    @staticmethod
    async def create_connection(loop, protocol, host, port):
        """
        Leave in a separate class so it can be easily Mocked,
        """
        await loop.create_connection(lambda: protocol, host, port)

    async def _reconnect_task(self):
        interval = 5
        self._log.info("Reconnect Task Started...")
        while True:
            try:
                await self._connect()
                return  # cancel task
            except Exception as e:
                self._log.exception(f"Reconnect Failed... Retrying in {interval} seconds", e)
                await asyncio.sleep(interval)

    def sendMsg(self, msg: bytes) -> None:
        """
        messages output from self.eclient are sent here
        """
        self.protocol.write(msg)

    def close(self):
        """
        Not implemented / untested
        """
        self.protocol.transport.close()

    def __repr__(self):
        return "Connection(host=%r, port=%r)" % (self.host, self.port)
