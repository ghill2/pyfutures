from pyfutures.logger import LoggerAdapter

class Handshake:



class Reader:
    def __init__(self, is_connected: asyncio.Event):
        self._log = LoggerAdapter.from_name(name=type(self).__name__)
        self.reader = StreamReader(limit=limit, loop=loop)

    async def read(self):
        """
            called from the _read_task() continuously after handshake / is_connected
            and from the connect() for the handshake
        """

        buf = b""
        try:
            data = await self._reader.read(4096)
            # self._log.debug(f"<-- {data}")
        except ConnectionResetError as e:
            self._log.debug(f"TWS closed the connection {e!r}...")
            self._is_connected.clear()
            return

        if len(data) == 0:
            self._log.debug("0 bytes received from server, connect has been dropped")
            self._is_connected.clear()
            return

        buf += data

        while len(buf) > 0:
            (size, msg, buf) = comm.read_msg(buf)
            self._log.debug(f"<-- {size}: {msg}")

            if msg:
                try:
                    self._handle_msg(msg)
                except Exception as e:
                    self._log.exception("_listen callback exception, _listen task still running...", e)
                # await asyncio.sleep(0)

            else:
                self._log.debug("more incoming packet(s) are needed ")
                break


    def _read_task():
        try:
            while True:
                await asyncio.sleep(0)
        except Exception as e:
            self._log.exception("_listen task exception, _listen task cancelled", e)






class Writer:
    def _process_write_queue_task():
        """
            process outgoing msg queue
        """
        async def _process_outgoing_msg_queue(self) -> None:
            while True:
                if self.connection.is_connected:
                    while not self._outgoing_msg_queue.empty():
                        msg: bytes = self._outgoing_msg_queue.get()
                        self._connection.sendMsg(msg)
                    await asyncio.sleep(0)
                else:
                    self._log.debug("Stopping outgoing messages, the client is disconnected. Waiting for 5 seconds...")
                    await asyncio.sleep(5)

    def write():
        """ibapi: client.sendMsg()"""
        self._log.debug(f"--> {msg}")
        self._writer.write(msg)
        self.loop.create_task(self._writer.drain())



class Connect:
    def __init__(self, reader, writer):
        

    def connect():
        self._outgoing_msg_task = self._loop.create_task(
            coro=self._process_outgoing_msg_queue(),
            name="outgoing_message_queue",
        )
        # probably need to clear the read buffer here first
        self.write()
        await self._reader.read()

