import asyncio
import logging
from datetime import datetime
from typing import ClassVar, Optional
from dotenv import dotenv_values

import aiodocker


class Gateway:
    IMAGE: ClassVar[str] = "ghcr.io/gnzsnz/ib-gateway:stable"
    CONTAINER_NAME: ClassVar[str] = "ib-gateway"

    def __init__(self, log_level: int = logging.DEBUG):
        self._container: Optional[aiodocker.containers.DockerContainer] = None
        self.log = logging.getLogger("Gateway")
        self.log.setLevel(log_level)

    async def create_container(self):
        """
        Creates/bootstraps a new IBC docker container on the local system (Docker Desktop).
        Deletes the container if it exists already.
        Does not start the container, only creates it
        """

        _docker = aiodocker.Docker()
        host = "127.0.0.1"
        c = await self.container()

        if c:
            await self.stop()

        tws_userid = dotenv_values()["TWS_USERNAME"]
        tws_password = dotenv_values()["TWS_PASSWORD"]

        # https://github.com/aio-libs/aiodocker/issues/829
        config=dict(
            Image="ghcr.io/gnzsnz/ib-gateway:stable",
            Restart_policy={"Name": "always"},
            # detach=True,
            Platform="amd64",
            ExposedPorts={
                "4001/tcp": {},
                "4002/tcp": {},
                "5900/tcp": {},
            },
            # Ports={
            #     "4003": {host, 4001),
            #     "4004": {host, 4002),
            #     "5900": {host, 5900,
            # },
            Env=[
            "TRADING_MODE=paper",
            f"TWS_USERID={tws_userid}",
            f"TWS_PASSWORD={tws_password}",
            "READ_ONLY_API=yes"
            ]
            )


        c = await _docker.containers.create(
            name=self.CONTAINER_NAME, 
            config=config
        )

        # await self.wait_until_login(c)
        await c.start()
        return c

    async def wait_until_login(self, c) -> bool:
        """
        Checks the docker container logs for a given bytestring.
        """
        self.log.info("Waiting until Gateway has (re)started........")
        # to prevent aiodocker from erroring, datetime is converted to seconds epoch
        now = datetime.now()
        now_seconds_epoch = int(now.timestamp())
        async for chunk in c.log(follow=True,stdout=True, stderr=True, since=now_seconds_epoch):
            if "IBC: Login has completed" in chunk:
                # self.log.info(chunk)
                self.log.info("Gateway Started and Ready... Continuing...")
                break

    async def start(self, wait: bool = True):
        """
        Starts the container if it's not already running.
        """

        c = await self.container()

        if not c:
            c = await self.create_container()


        self.log.info("Starting Gateway...")
        metadata = await c.show()
        if metadata["State"]["Status"] == "running":
            self.log.info("start() called when container is already running...")
            return

        await c.start()  # Use aiodocker's async start()
        if wait:
            await self.wait_until_login(c)

    async def restart(self):
        """
        Restarts the container.
        """
        c = await self.container()
        await c.restart()  # Use aiodocker's async restart()
        await self.wait_until_login(c)

    async def stop(self):
        """
        Stops the container and waits for it to shut down completely.
        """
        c = await self.container()
        if c:
            self.log.info("Stopping Gateway...")
            await c.stop()
            await c.wait()
            self._container = None

    async def container(self) -> Optional[aiodocker.containers.DockerContainer]:
        # ... (rest of the code remains the same)
        _docker = aiodocker.Docker()
        containers = await _docker.containers.list(all=True)

        # Correctly await the async iterator for containers:
        for c in containers:
            metadata = await c.show()
            if metadata["Name"] == f"/{self.CONTAINER_NAME}":
                return c

        return None


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    gateway = Gateway()
    loop.run_until_complete(gateway.create_container())
