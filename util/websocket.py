import asyncio
import json
import logging

import websockets
from motor.motor_asyncio import AsyncIOMotorClient

from util.app import getDb, loadDrivers, runReplay
from model.packets import makeDriversPacket, makeErrorPacket, makeStatusPacket
from model.replay import StreamHandle, ReplayControl

log = logging.getLogger(__name__)


def parseStartRequest(req) -> dict:
    if req.get("type") != "start":
        raise ValueError(f"Unknown request type: {req.get('type')!r}")

    mode = req.get("mode")
    if mode not in ("live", "replay"):
        raise ValueError(f"Mode must be 'live' or 'replay', got {mode!r}")

    sessionKey = req.get("session_key")
    if not isinstance(sessionKey, int):
        raise ValueError("session_key is required and must be an integer")

    speed = req.get("speed")
    if speed is not None and not isinstance(speed, (int, float)):
        raise ValueError("speed must be a number if provided")

    return {"mode": mode, "session_key": sessionKey, "speed": speed}


async def handleClient(
    ws: websockets.WebSocketServerProtocol, mongoClient: AsyncIOMotorClient
) -> None:
    remote = ws.remote_address
    log.info(f"Client connected: {remote}")

    db = getDb(mongoClient)
    stream = StreamHandle()

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception as e:
                log.warning(f"Bad request from {remote}: {e}")
                await ws.send(makeErrorPacket(str(e)))
                continue

            msgType = msg.get("type")

            if msgType == "start":
                try:
                    req = parseStartRequest(msg)
                except Exception as e:
                    log.warning(f"Bad start request from {remote}: {e}")
                    await ws.send(makeErrorPacket(f"Bad start request from {remote}: {e}"))
                    continue

                if stream.task is not None and not stream.task.done():
                    stream.task.cancel()
                    try:
                        await stream.task
                    except asyncio.CancelledError:
                        pass
                stream.control = None

                sessionKey = msg.get("session_key")
                try:
                    drivers = await loadDrivers(db, sessionKey)
                    await ws.send(makeDriversPacket(drivers))
                    await ws.send(
                        makeStatusPacket("starting", mode=req["mode"], session_key=sessionKey)
                    )

                    coro = runReplay(db, ws, sessionKey, handle=stream)
                    replayControl = ReplayControl(speed = req["speed"])

                    stream.task = asyncio.create_task(coro)
                    stream.control = replayControl
                except Exception as e:
                    log.warning(f"Failed to start stream for {remote}: {e}")
                    await ws.send(makeErrorPacket(f"start failed: {e}"))
                    
            elif msgType == "control":
                log.info(f"Remote {remote} requested speed update to {msg.get("speed")}")

                if stream.task is None:
                    await ws.send(makeErrorPacket("No stream running"))
                    continue

                if not stream.task.done() and stream.control is not None:
                    stream.control.setSpeed(msg.get("speed"))
                    await ws.send(makeStatusPacket(f"Stream speed updated to {msg.get("speed")}"))
            else:
                log.warning(f"Unknown packet type from {remote}: {msgType}")

    except websockets.ConnectionClosed:
        pass
    finally:
        if stream.task is not None and not stream.task.done():
            stream.task.cancel()
        log.info(f"Client disconnected: {remote}")
