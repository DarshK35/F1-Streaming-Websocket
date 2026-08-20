import time
import asyncio
import dataclasses

@dataclasses.dataclass
class ReplayControl:
    simStart: float
    speed: float = 1
    _simElapsed: float = 0
    _lastRealTime: float = dataclasses.field(default_factory=time.monotonic)

    def tick(self) -> float:
        now = time.monotonic()
        realDelta = now - self._lastRealTime
        self._lastRealTime = now
        self._simElapsed += realDelta * self.speed
        return self.simStart + self._simElapsed

    def setSpeed(self, speed: float) -> None:
        self.tick()
        self.speed = speed

    @property
    def paused(self) -> bool:
        return self.speed == 0.0

class StreamHandle:
    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.control: ReplayControl | None = None