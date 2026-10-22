"""Fraud rules. Pure logic, no Kafka, so it is easy to test.

All time is *event time* (the timestamp on the transaction), not wall clock.
Each user has their own small state; Kafka keys by user_id, so one consumer
always sees one user's events in order.
"""
import math
from collections import deque

from .common import CITIES, haversine_km

# Impossible travel: faster than a plane, and far enough to matter.
MAX_SPEED_KMH = 900
MIN_TRAVEL_KM = 500

# Velocity: too many swipes in a sliding window.
VELOCITY_WINDOW_S = 10
VELOCITY_MAX_TX = 6

# Amount: far above this user's own normal.
AMOUNT_MIN_SAMPLES = 5
AMOUNT_SIGMAS = 4
AMOUNT_MIN_RATIO = 5

# Don't repeat the same alert for the same user every event.
COOLDOWN_S = 30


class UserState:
    def __init__(self):
        self.window = deque()  # (ts, amount) inside the velocity window
        self.last = None  # last tx dict, for travel check
        # Welford running mean/variance of amounts
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.last_alert = {}  # rule -> ts

    def _std(self):
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0

    def _cooled(self, rule, ts):
        prev = self.last_alert.get(rule)
        if prev is not None and ts - prev < COOLDOWN_S:
            return False
        self.last_alert[rule] = ts
        return True

    def process(self, tx):
        """Feed one transaction, return a list of alert dicts (maybe empty)."""
        ts, amount = tx["ts"], tx["amount"]
        alerts = []

        # 1. impossible travel
        prev = self.last
        if prev and prev["city"] != tx["city"] and ts > prev["ts"]:
            km = haversine_km(CITIES[prev["city"]], CITIES[tx["city"]])
            hours = (ts - prev["ts"]) / 3600
            speed = km / hours
            if km >= MIN_TRAVEL_KM and speed > MAX_SPEED_KMH:
                if self._cooled("impossible_travel", ts):
                    alerts.append(
                        _alert(
                            tx,
                            "impossible_travel",
                            f"{prev['city']} -> {tx['city']} ({km:.0f} km) in "
                            f"{(ts - prev['ts']) / 60:.1f} min",
                        )
                    )

        # 2. velocity (sliding window)
        self.window.append((ts, amount))
        while self.window and ts - self.window[0][0] > VELOCITY_WINDOW_S:
            self.window.popleft()
        if len(self.window) >= VELOCITY_MAX_TX and self._cooled("velocity", ts):
            alerts.append(
                _alert(
                    tx,
                    "velocity",
                    f"{len(self.window)} transactions in {VELOCITY_WINDOW_S}s",
                )
            )

        # 3. amount spike, judged against history *before* this tx
        if self.n >= AMOUNT_MIN_SAMPLES:
            limit = self.mean + AMOUNT_SIGMAS * self._std()
            if amount > limit and amount > AMOUNT_MIN_RATIO * self.mean:
                if self._cooled("amount_spike", ts):
                    alerts.append(
                        _alert(
                            tx,
                            "amount_spike",
                            f"${amount:,.2f} vs typical ${self.mean:,.2f}",
                        )
                    )

        # Update state. Skip flagged amounts so one spike can't skew "normal".
        if not any(a["rule"] == "amount_spike" for a in alerts):
            self.n += 1
            d = amount - self.mean
            self.mean += d / self.n
            self.m2 += d * (amount - self.mean)
        if prev is None or ts >= prev["ts"]:
            self.last = tx
        return alerts


def _alert(tx, rule, detail):
    return {
        "rule": rule,
        "detail": detail,
        "user_id": tx["user_id"],
        "tx_id": tx["tx_id"],
        "amount": tx["amount"],
        "city": tx["city"],
        "ts": tx["ts"],
    }
