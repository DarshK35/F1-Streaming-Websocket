import asyncio
import logging
from collections import deque
from datetime import datetime

import websockets
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

import util.config as configMod
from model.packets import makeCarStatusPacket, makeRaceControlPacket
from model.replay import StreamHandle, ReplayControl

log = logging.getLogger(__name__)
STREAM_SCHEMAS = ["car-location", "car-telemetry", "race-control"]


# Dynamic DB Reader
def getDb(client: AsyncIOMotorClient):
    return client[configMod.config["mongo"]["db-name"]]


# ########################################
# Server State Update Helpers
# ########################################
def locationUpdate(doc: dict, state: dict[str, dict | list]):
    assert isinstance(state["car-status"], dict)
    car = state["car-status"].setdefault(doc["driver_number"], {})
    car["location"] = {
        "x": doc["x"],
        "y": doc["y"],
        "z": doc["z"],
        "t": doc["date"].timestamp(),
    }


def telemetryUpdate(doc: dict, state: dict[str, dict | list]):
    assert isinstance(state["car-status"], dict)
    car = state["car-status"].setdefault(doc["driver_number"], {})
    car["telemetry"] = {
        "rpm": doc["rpm"],
        "speed": doc["speed"],
        "gear": doc["n_gear"],
        "throttle": doc["throttle"],
        "brake": doc["brake"],
        "drs": doc["drs"],
        "t": doc["date"].timestamp(),
    }


def raceControlUpdate(doc: dict, state: dict[str, dict | list]):
    assert isinstance(state["race-control"], list)
    state["race-control"].append(doc)


HANDLERS = {
    "car-location": locationUpdate,
    "car-telemetry": telemetryUpdate,
}


# ########################################
# General Data Loaders
# ########################################
async def loadDrivers(db, sessionKey: int) -> list[dict]:
    coll = db[configMod.config["mongo"]["schemas"]["drivers"]]
    cursor = coll.find({"session_key": sessionKey})
    drivers = [doc async for doc in cursor]
    log.info(f"Loaded {len(drivers)} drivers for session {sessionKey}")
    return drivers


# ########################################
# Playback simulations
# ########################################
# TODO: Implement Live race watch
async def watchLive(
    db: AsyncIOMotorDatabase, ws: websockets.WebSocketServerProtocol, sessionKey: int
) -> None:
    INTERVAL = 1 / configMod.config["replay"]["tick-rate"]
    print(INTERVAL)


async def runReplay(
    db: AsyncIOMotorDatabase,
    ws: websockets.WebSocketServerProtocol,
    sessionKey: int,
    handle: StreamHandle,
) -> None:
    collectors = {
        key: db[configMod.config["mongo"]["schemas"][key]] for key in STREAM_SCHEMAS
    }
    cursors = {
        key: collectors[key].find({"session_key": sessionKey}) for key in STREAM_SCHEMAS
    }

    assert handle.control is not None
    log.info(f"Replaying session {sessionKey} at {handle.control.speed}x speed")
    INTERVAL = 1 / configMod.config["replay"]["tick-rate"]

    buffers = {key: deque() for key in STREAM_SCHEMAS}
    bufferCompleteEvents = {key: asyncio.Event() for key in STREAM_SCHEMAS}

    # Async Stream helpers
    async def getEarliestTimestamp() -> float:
        firsts = await asyncio.gather(
            *(
                collectors[schema]
                .find({"session_key": sessionKey})
                .sort("date", 1)
                .limit(1)
                .to_list(1)
                for schema in STREAM_SCHEMAS
            )
        )
        dates = [docs[0]["date"].timestamp() for docs in firsts if docs]
        return min(dates) if dates else -1

    async def readBuffer(coll: str):
        async for doc in cursors[coll]:
            buffers[coll].append(doc)
        bufferCompleteEvents[coll].set()

    async def streamBuffers(control: ReplayControl):
        count = 0
        serverState: dict[str, dict | list] = {"car-status": {}, "race-control": []}

        while True:
            await asyncio.sleep(INTERVAL)

            simNow = control.tick()

            # Process buffers
            carDirty = False
            serverState["race-control"] = []
            for schema in STREAM_SCHEMAS:
                buffer = buffers[schema]
                while buffer and buffer[0]["date"].timestamp() <= simNow:
                    doc = buffer.popleft()
                    if schema in ["car-location", "car-telemetry"]:
                        HANDLERS[schema](doc, serverState)
                        carDirty = True
                    if schema == "race-control":
                        raceControlUpdate(doc, serverState)

            if carDirty:
                assert isinstance(serverState["car-status"], dict)
                packet = makeCarStatusPacket(serverState["car-status"])
                await ws.send(packet)
                count += 1
            if serverState["race-control"]:
                await ws.send(makeRaceControlPacket(serverState["race-control"]))
                count += 1

            if all(
                [event.is_set() for event in bufferCompleteEvents.values()]
            ) and not any(buffers.values()):
                log.info(
                    f"Replay finished for session {sessionKey} ({count} packets sent)"
                )
                return

    simStart: float = await getEarliestTimestamp()
    handle.control.simStart = simStart

    await asyncio.gather(
        *(readBuffer(schema) for schema in STREAM_SCHEMAS), streamBuffers(handle.control)
    )
