import asyncio
import functools
import logging

import websockets
from motor.motor_asyncio import AsyncIOMotorClient

import util.config as configMod
from util.config import loadConfig
from util.websocket import handleClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


async def main():
    loadConfig()

    mongoClient = AsyncIOMotorClient(configMod.config["mongo"]["connection-str"])

    host = configMod.config["server"]["host"]
    port = configMod.config["server"]["port"]

    handler = functools.partial(handleClient, mongoClient=mongoClient)

    async with websockets.serve(
        handler, host, port, ping_interval=None, ping_timeout=None
    ):
        log.info(
            f"Websocket server listening on {host}:{port}, awaiting client requests"
        )
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
