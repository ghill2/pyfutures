import datetime
import logging
import time
from typing import ClassVar

import docker
from dotenv import dotenv_values


# logging.basicConfig(stream=sys.stdout, level=logging.WARN)


class Gateway:
    IMAGE: ClassVar[str] = "ghcr.io/gnzsnz/ib-gateway:stable"
    CONTAINER_NAME: ClassVar[str] = "ib-gateway"

    def __init__(self, log_level: int = logging.DEBUG):
        self._docker = docker.from_env()
        # self._container = None
        self.log = logging.getLogger("Gateway")
        self.log.setLevel(log_level)

    def create_container(self):
        """
        Creates / bootstraps new ibc docker container on the local system (Docker Desktop)
        deletes the container if it exists already
        """
        host = "127.0.0.1"
        c = self.container()

        if c:
            c.stop()
            c.remove()

        c = self._docker.containers.run(
            image=self.IMAGE,
            name=self.CONTAINER_NAME,
            restart_policy={"Name": "always"},
            detach=True,
            ports={
                "4003": (host, 4001),
                "4004": (host, 4002),
                "5900": (host, 5900),
            },
            platform="amd64",
            environment={
                "TWS_USERID": dotenv_values()["TWS_USERNAME"],
                "TWS_PASSWORD": dotenv_values()["TWS_PASSWORD"],
                "TRADING_MODE": "paper",
                "READ_ONLY_API": {True: "yes", False: "no"}[True],
            },
        )

        self.wait_until_login(c)

    def wait_until_login(self, c, timeout: int = 120) -> bool:
        """
        checks the docker container logs for a given bytestring
        """
        now = datetime.datetime.now()
        self.log.info("Waiting until Gateway has (re)started........")
        for _ in range(timeout):
            logs = c.logs(since=now)
            bytes = b"IBC: Login has completed"
            if any(bytes in line for line in logs.split(b"\n")):
                self.log.info("Gateway Started and Ready... Continuing...")
                return True
            time.sleep(1)
        raise Exception("wait_until_login: Cannot (re)start Container, exiting...")

    def start(self):
        """
        This method only applies if the
        """
        c = self.container()
        if c.status == "running":
            # raise Exception("start() called when container is already running, start() should be executed after stop()")
            self.log.info("start() called when container is already running...")
            return True
        c.start()
        self.wait_until_login(c)

    def restart(self):
        """
        restarts the container
        """
        c = self.container()
        c.restart()
        self.wait_until_login(c)

    def stop(self):
        """
        stops the container and blocks until the container has shut down completely
        this method returns AFTER Gateway has sent an empty bytestring to the client
        """
        c = self.container()
        c.stop()
        c.wait()

    def container(self):
        """
        Queries the docker containers on the system and checks if the container exists
        Necessary to restore state after a test restart
        """
        all_containers = {c.name: c for c in self._docker.containers.list(all=True)}
        container = all_containers.get(self.CONTAINER_NAME)
        return container


class NoContainer(Exception):
    pass


gateway = Gateway()
gateway.stop()
# gateway.start()
# gateway.restart()
# gateway.restart()
