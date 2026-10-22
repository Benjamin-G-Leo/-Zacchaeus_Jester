"""Fraud rules. Pure logic, no Kafka or Redis, so it is easy to test.

All time is *event time* (the timestamp on the transaction), not wall clock.
`evaluate` looks at one transaction plus what we remember about the user
(`UserState`) and returns alerts. Where that memory lives (Python dict or
Redis) is decided in `state.py`.
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
    """What the rules remember about one user, apart from the velocity window."""

    def __init__(self):
        self.last_ts = None
        self.last_city = None
        # Welford running mean/variance of amounts
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.last_alert = {}  # rule -> ts
        self._window = deque()  # in-memory velocity window (Redis uses a sorted set)

    def std(self):
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0

    def cooled(self, rule, ts):
        prev = self.last_alert.get(rule)
        if prev is not None and ts - prev < COOLDOWN_S:
            return False
        self.last_alert[rule] = ts
        return True

    def process(self, tx):
        """In-memory path: track the velocity window here, then run the rules."""
        ts = tx["ts"]
        self._window.append(ts)
        while self._window and ts - self._window[0] > VELOCITY_WINDOW_S:
            self._window.popleft()
        return evaluate(self, tx, len(self._window))

    def to_fields(self):
        f = {"n": self.n, "mean": self.mean, "m2": self.m2}
        if self.last_ts is not None:
            f["last_ts"] = self.last_ts
            f["last_city"] = self.last_city
        for rule, ts in self.last_alert.items():
            f["cd:" + rule] = ts
        return f

    @classmethod
    def from_fields(cls, fields):
        s = cls()
        if not fields:
            return s
        s.n = int(fields.get("n", 0))
        s.mean = float(fields.get("mean", 0))
        s.m2 = float(fields.get("m2", 0))
        if "last_ts" in fields:
            s.last_ts = float(fields["last_ts"])
            s.last_city = fields["last_city"]
        s.last_alert = {k[3:]: float(v) for k, v in fields.items() if k.startswith("cd:")}
        return s


def evaluate(s, tx, window_count):
    """Run all rules for one transaction and update `s`.

    window_count is how many of this user's transactions (including this one)
    fall inside the velocity window. The caller owns that window.
    """
    ts, amount, city = tx["ts"], tx["amount"], tx["city"]
    alerts = []

    # 1. impossible travel
    if s.last_city is not None and s.last_city != city and ts > s.last_ts:
        km = haversine_km(CITIES[s.last_city], CITIES[city])
        speed = km / ((ts - s.last_ts) / 3600)
        if km >= MIN_TRAVEL_KM and speed > MAX_SPEED_KMH and s.cooled("impossible_travel", ts):
            alerts.append(
                _alert(
                    tx,
                    "impossible_travel",
                    f"{s.last_city} -> {city} ({km:.0f} km) in {(ts - s.last_ts) / 60:.1f} min",
                    from_city=s.last_city,
                )
            )

    # 2. velocity (sliding window)
    if window_count >= VELOCITY_MAX_TX and s.cooled("velocity", ts):
        alerts.append(_alert(tx, "velocity", f"{window_count} transactions in {VELOCITY_WINDOW_S}s"))

    # 3. amount spike, judged against history *before* this tx
    if s.n >= AMOUNT_MIN_SAMPLES:
        limit = s.mean + AMOUNT_SIGMAS * s.std()
        if amount > limit and amount > AMOUNT_MIN_RATIO * s.mean and s.cooled("amount_spike", ts):
            alerts.append(_alert(tx, "amount_spike", f"${amount:,.2f} vs typical ${s.mean:,.2f}"))

    # Update state. Skip flagged amounts so one spike can't skew "normal".
    if not any(a["rule"] == "amount_spike" for a in alerts):
        s.n += 1
        d = amount - s.mean
        s.mean += d / s.n
        s.m2 += d * (amount - s.mean)
    if s.last_ts is None or ts >= s.last_ts:
        s.last_ts, s.last_city = ts, city
    return alerts


def _alert(tx, rule, detail, from_city=None):
    a = {
        "rule": rule,
        "detail": detail,
        "user_id": tx["user_id"],
        "tx_id": tx["tx_id"],
        "amount": tx["amount"],
        "city": tx["city"],
        "ts": tx["ts"],
    }
    if from_city:
        a["from_city"] = from_city
    return a
