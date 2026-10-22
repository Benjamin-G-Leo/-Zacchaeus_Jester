"""Shared config and helpers."""
import json
import math
import os

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_TX = "transactions"
TOPIC_ALERTS = "fraud-alerts"

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
    return 2 * 6371 * math.asin(math.sqrt(h))


def dumps(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def loads(raw: bytes):
    return json.loads(raw)
