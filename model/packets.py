import json

# ########################################
# General Data Packets
# ########################################
def makeDriversPacket(drivers: list[dict]) -> str:
    return json.dumps(
        {
            "type": "drivers",
            "drivers": [
                {
                    "number": d["driver_number"],
                    "acronym": d.get("name_acronym", "???"),
                    "team_colour": d.get("team_colour", "FFFFFF"),
                    "full_name": d.get("full_name", ""),
                }
                for d in drivers
            ],
        }
    )


# ########################################
# Replay Packets
# ########################################
def makeCarStatusPacket(carStatus: dict[int, dict]) -> str:
    return json.dumps(
        {
            "type": "car-status",
            "cars": [{"driver": num, **car} for num, car in carStatus.items()],
        }
    )

def makeRaceControlPacket(events: list[dict]) -> str:
    return json.dumps(
        {
            "type": "race-control",
            "events": [
                {
                    "driver": e.get("driver_number"),
                    "lap": e.get("lap_number"),
                    "category": e.get("category"),
                    "flag": e.get("flag"),
                    "scope": e.get("scope"),
                    "sector": e.get("sector"),
                    "message": e.get("message"),
                    "t": e["date"].isoformat(),
                }
                for e in events
            ],
        }
    )


# ########################################
# Miscellaneous Packets
# ########################################
def makeStatusPacket(state: str, **extra) -> str:
    return json.dumps({"type": "status", "state": state, **extra})

def makeErrorPacket(error: str) -> str:
    return json.dumps({"type": "error", "message": error})
