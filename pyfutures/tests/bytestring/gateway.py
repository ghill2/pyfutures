import asyncio
import logging
from datetime import datetime
from typing import ClassVar

import aiodocker
from dotenv import dotenv_values

from pyfutures.logger import LoggerAdapter


# https://github.com/aio-libs/aiodocker/issues/829
# copied from nautilus_trader IB gateway when docker container.create() is called
# this is the config sent in the request to the docker server endpoint
#
tws_userid = dotenv_values()["TWS_USERNAME"]
tws_password = dotenv_values()["TWS_PASSWORD"]
CREATE_CONFIG = {
    "Hostname": None,
    "Domainname": None,
    "ExposedPorts": {"4003/tcp": {}, "4004/tcp": {}, "5900/tcp": {}},
    "User": None,
    "Tty": False,
    "OpenStdin": False,
    "StdinOnce": False,
    "AttachStdin": False,
    "AttachStdout": False,
    "AttachStderr": False,
    "Env": [
        f"TWS_USERID={tws_userid}",
        f"TWS_PASSWORD={tws_password}",
        "TRADING_MODE=paper",
        "READ_ONLY_API=yes",
    ],
    "Cmd": None,
    "Image": "ghcr.io/gnzsnz/ib-gateway:stable",
    "Volumes": None,
    "NetworkDisabled": False,
    "Entrypoint": None,
    "WorkingDir": None,
    "HostConfig": {
        "NetworkMode": "default",
        "RestartPolicy": {"Name": "always"},
        "PortBindings": {
            "4003/tcp": [{"HostIp": "127.0.0.1", "HostPort": "4001"}],
            "4004/tcp": [{"HostIp": "127.0.0.1", "HostPort": "4002"}],
            "5900/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5900"}],
        },
    },
    "NetworkingConfig": None,
    "MacAddress": None,
    "Labels": None,
    "StopSignal": None,
    "Healthcheck": None,
    "StopTimeout": None,
    "Runtime": None,
}


class Gateway:
    CONTAINER_NAME: ClassVar[str] = "ib-gateway"

    def __init__(self, log_level: int = logging.DEBUG):
        self._container: aiodocker.containers.DockerContainer | None = None
        self._log = LoggerAdapter.from_name(name=type(self).__name__)

    async def create_container(self):
        """
        Creates/bootstraps a new IBC docker container on the local system (Docker Desktop).
        Deletes the container if it exists already.
        Does not start the container, only creates it
        """
        _docker = aiodocker.Docker()
        c = await self.container()

        if c:
            await self.stop()

        c = await _docker.containers.create(
            name=self.CONTAINER_NAME, config=CREATE_CONFIG
        )

        return c

    async def wait_until_login(self, c) -> bool:
        """
        Checks the docker container logs for a given bytestring.
        """
        self._log.info("Waiting until Gateway has (re)started........")
        # to prevent aiodocker from erroring, datetime is converted to seconds epoch
        now = datetime.now()
        now_seconds_epoch = int(now.timestamp())
        async for chunk in c.log(
            follow=True, stdout=True, stderr=True, since=now_seconds_epoch
        ):
            if "IBC: Login has completed" in chunk:
                # self._log.info(chunk)
                self._log.info("Gateway Started and Ready... Continuing...")
                break

    async def start(self, wait: bool = True):
        """
        Starts the container if it's not already running.
        """
        c = await self.container()

        if not c:
            c = await self.create_container()

        self._log.info("Starting Gateway...")
        metadata = await c.show()
        if metadata["State"]["Status"] == "running":
            self._log.info("start() called when container is already running...")
            return

        await c.start()  # Use aiodocker's async start()
        if wait:
            await self.wait_until_login(c)

    async def restart(self):
        """
        Restarts the container.
        """
        c = await self.container()
        self._log.info(f"Restarting Container {c}...")
        await c.restart()  # Use aiodocker's async restart()
        await self.wait_until_login(c)

    async def stop(self):
        """
        Stops the container and waits for it to shut down completely.
        """
        c = await self.container()
        if c:
            self._log.info("Stopping Gateway...")
            await c.stop()
            await c.wait()
            self._container = None

    async def container(self) -> aiodocker.containers.DockerContainer | None:
        # ... (rest of the code remains the same)
        _docker = aiodocker.Docker()
        containers = await _docker.containers.list(all=True)

        # Correctly await the async iterator for containers:
        for c in containers:
            metadata = await c.show()
            if metadata["Name"] == f"/{self.CONTAINER_NAME}":
                return c

        return None


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    gateway = Gateway()
    loop.run_until_complete(gateway.create_container())


# @classmethod
# async def start_tws(cls):
#     print("Starting tws...")
#     if cls.is_tws_running():
#         await cls.kill_tws()
#     os.system("sh /opt/ibc/twsstartmacos.sh")
#
#     while not cls.is_tws_running():
#         print("Waiting for tws to open...")
#         await asyncio.sleep(1)
#
# @classmethod
# async def kill_tws(cls):
#     print("Killing tws...")
#     os.system("killall -m java")
#     os.system("killall -m Trader Workstation 10.26")
#     while cls.is_tws_running():
#         print("Waiting for tws to close...")
#         await asyncio.sleep(1)
#
# @staticmethod
# def is_tws_running() -> bool:
#     for process in psutil.process_iter(["pid", "name"]):
#         name = process.info["name"].lower()
#         if name == "java" or name.startswith("Trader Workstation"):
#             return True
#     return False
