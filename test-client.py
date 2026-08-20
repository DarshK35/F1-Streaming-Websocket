import argparse
import asyncio
import json
import logging
import time

import websockets

config = {
    "server": {"host": "localhost", "port": 8000},
    "mode": "replay",
    "session_key": 9912,
    "speed": 10.0,
    "verbose": False,
    "summary_interval": 2.0,
}
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def parseArguments():
    parser = argparse.ArgumentParser("F1 Websocket Test Client")
    parser.add_argument("--host", help="Server host")
    parser.add_argument("--port", type=int, help="Server port")
    parser.add_argument(
        "--mode",
        choices=["live", "replay"],
        default="replay",
        help="Stream mode to request",
    )
    parser.add_argument(
        "--session-key",
        type=int,
        help="Session key to request",
    )
    parser.add_argument(
        "--speed",
        type=float,
        help="Replay speed multiplier",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every position packet instead of a periodic summary",
    )

    global config
    args = vars(parser.parse_args())
    if args["host"] is not None:
        config["server"]["host"] = args["host"]
    if args["port"] is not None:
        config["server"]["port"] = args["port"]
    if args["mode"] is not None:
        config["mode"] = args["mode"]
    if args["session_key"] is not None:
        config["session_key"] = args["session_key"]
    if args["speed"] is not None:
        config["speed"] = args["speed"]
    if args["verbose"] is not None:
        config["verbose"] = args["verbose"]


class Summary:
    def __init__(self) -> None:
        self.totalCount: int = 0
        self.windowCount: int = 0
        self.perDriver: dict[int, int] = {}
        self.windowStart = time.monotonic()

    def record(self, driver: int) -> None:
        self.totalCount += 1
        self.windowCount += 1
        self.perDriver[driver] = self.perDriver.get(driver, 0) + 1

    def maybePrint(self) -> None:
        elapsed = time.monotonic() - self.windowStart
        if elapsed < config["summary_interval"]:
            return
        rate = self.windowCount / elapsed
        log.info(
            f"{self.totalCount} packets received "
            f"({rate:.1f}/s, {len(self.perDriver)} drivers active)"
        )
        self.windowCount = 0
        self.windowStart = time.monotonic()


async def run():
    uri = f"ws://{config['server']['host']}:{config['server']['port']}"
    log.info(f"Connecting to {uri}")

    async with websockets.connect(uri) as ws:
        request = {
            "type": "start",
            "mode": config["mode"],
            "session_key": config["session_key"],
            "speed": config["speed"],
        }

        log.info(f"Sending request: {request}")
        await ws.send(json.dumps(request))

        summary = Summary()
        async for raw in ws:
            packet = json.loads(raw)
            packetType = packet.get("type")

            if packetType == "drivers":
                drivers = packet["drivers"]
                log.info(f"Driver roster received: {len(drivers)} drivers")
                if config["verbose"]:
                    for d in drivers:
                        log.info(
                            f"    {d['number']:>2} {d['full_name']} ({d['acronym']})"
                        )
            elif packetType == "status":
                log.info(f"Server status: {packet}")
            elif packetType == "error":
                log.error(f"Server error: {packet['message']}")
            elif packetType == "position":
                if config["verbose"]:
                    log.info(
                        f"position driver = {packet['driver']} "
                        f"x={packet['x']} y={packet['y']} z={packet['z']} t={packet['t']}"
                    )
                summary.record(packet["driver"])
                if not config["verbose"]:
                    summary.maybePrint()
            else:
                log.warning(f"Unknown packet type: {packetType}")


def main():
    parseArguments()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
        log.error(f"Connection error: {e}")


if __name__ == "__main__":
    main()
