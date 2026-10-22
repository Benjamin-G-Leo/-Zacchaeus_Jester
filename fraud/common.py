"""Shared config and helpers."""
import json
import math
import os
from pathlib import Path


def load_dotenv(path):
    """Read KEY=VALUE lines from a .env file into the environment.

    Real environment variables win, so nothing here can override what you set yourself.
    Missing file is fine. Passwords live in .env, which git ignores.
    """
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")  # empty means the dashboard is open
TOPIC_TX = "transactions"
TOPIC_ALERTS = "fraud_alerts"
TOPIC_DLQ = "transactions_dlq"  # dead letters: messages the detector could not use
RULES = ("impossible_travel", "velocity", "amount_spike")

# name -> (lat, lon)
CITIES = {
    "Mumbai": (19.07, 72.88),
    "Delhi": (28.61, 77.21),
    "Bangalore": (12.97, 77.59),
    "New York": (40.71, -74.01),
    "Los Angeles": (34.05, -118.24),
    "London": (51.51, -0.13),
    "Paris": (48.86, 2.35),
    "Tokyo": (35.68, 139.69),
    "Sydney": (-33.87, 151.21),
    "Dubai": (25.20, 55.27),
}


def haversine_km(a, b):
    """Great-circle distance in km between two (lat, lon) points."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    # min() guards float error pushing h a hair above 1 for near-antipodal points
    return 2 * 6371 * math.asin(min(1.0, math.sqrt(h)))


def dumps(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def loads(raw: bytes):
    return json.loads(raw)
