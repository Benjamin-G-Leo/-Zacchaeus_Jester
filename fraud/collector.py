"""Kafka to dashboard hand-off. No Streamlit in here, so it can be tested.

Streamlit re-runs the page script constantly, so nothing that reads Kafka may
live in it. One daemon thread owns a single consumer subscribed to both topics.
Alerts cross to the page through a thread-safe queue.Queue; the page calls
snapshot(), which drains that queue. Plain transactions are only counted (per
second, for the throughput sparkline). Counters are kept by the thread, so they
stay exact even if no page is open.
"""
import math
import queue
import threading
import time
from collections import Counter, deque

from kafka import KafkaConsumer

from .common import BOOTSTRAP, RULES, TOPIC_ALERTS, TOPIC_TX, loads
from .retry import connect_kafka

RATE_WINDOW_S = 10
HISTORY_S = 60  # seconds of throughput history, and the "active anomalies" window

_NUMBER_FIELDS = ("distance_km", "elapsed_s", "speed_kmh", "count", "window_s", "typical", "limit", "multiple", "samples")


def _is_number(v):
    return not isinstance(v, bool) and isinstance(v, (int, float)) and math.isfinite(v)


def parse_alert(raw):
    """Alert dict ready to show, or None. One malformed message must not break the page.

    The dashboard indexes these fields directly, so a missing one raised a KeyError on
    every redraw and froze the page for everyone until the server restarted.
    """
    try:
        a = loads(raw)
    except ValueError:
        return None
    if not isinstance(a, dict) or a.get("rule") not in RULES:
        return None
    for key in ("user_id", "tx_id", "city", "detail"):
        if not isinstance(a.get(key), str):
            return None
    if not _is_number(a.get("amount")) or not _is_number(a.get("ts")):
        return None
    for key in _NUMBER_FIELDS:  # optional detail numbers: keep only the ones that really are numbers
        if key in a and not _is_number(a[key]):
            del a[key]
    if "from_city" in a and not isinstance(a["from_city"], str):
        del a["from_city"]
    return a


class Collector:
    def __init__(self, start=True, inbox_max=5000, recent_max=600):
        self.inbox = queue.Queue(maxsize=inbox_max)  # reader thread -> page
        self.lock = threading.Lock()  # guards everything below
        self.recent = deque(maxlen=recent_max)  # newest first
        self.alert_times = deque()  # arrival time of each alert in the last minute
        self.by_rule = Counter()
        self.alert_total = 0
        self.tx_total = 0
        self.tx_buckets = deque()  # [epoch second, count], last minute only
        self.error = None
        self.rejected = 0  # alerts dropped because they were malformed
        if start:
            threading.Thread(target=self._keep_running, daemon=True).start()

    # ---- reader-thread side ----

    def put_alert(self, alert):
        now = time.time()
        with self.lock:
            self.by_rule[alert["rule"]] += 1
            self.alert_total += 1
            self.alert_times.append(now)
            self._trim_alert_times(now)
        while True:
            try:
                self.inbox.put_nowait(alert)
                return
            except queue.Full:  # nobody is draining (no page open): drop the oldest
                try:
                    self.inbox.get_nowait()
                except queue.Empty:
                    pass

    def count_tx(self):
        sec = int(time.time())
        with self.lock:
            self.tx_total += 1
            if self.tx_buckets and self.tx_buckets[-1][0] == sec:
                self.tx_buckets[-1][1] += 1
            else:
                self.tx_buckets.append([sec, 1])
            while self.tx_buckets and sec - self.tx_buckets[0][0] >= HISTORY_S:
                self.tx_buckets.popleft()

    def _trim_alert_times(self, now):
        while self.alert_times and now - self.alert_times[0] > HISTORY_S:
            self.alert_times.popleft()

    def _note_error(self, err):
        self.error = f"{type(err).__name__}: {err}"

    def _watch(self):
        # deadline_s=None: keep waiting for Kafka for as long as it takes
        consumer = connect_kafka(
            KafkaConsumer,
            TOPIC_ALERTS,
            TOPIC_TX,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
            deadline_s=None,
            on_retry=self._note_error,
        )
        self.error = None
        try:
            for rec in consumer:
                if rec.topic == TOPIC_TX:  # only counting, so skip JSON parsing
                    self.count_tx()
                    continue
                alert = parse_alert(rec.value)
                if alert is None:
                    self.rejected += 1
                    continue
                alert["seen_at"] = time.time()  # event time can be simulated future; show arrival time
                self.put_alert(alert)
        finally:
            consumer.close()  # a reconnect builds a new consumer; don't leak the old one

    def _keep_running(self):
        while True:  # if Kafka drops mid-stream, reconnect (connect_kafka backs off)
            try:
                self._watch()
            except Exception as e:  # noqa: BLE001
                self._note_error(e)
                time.sleep(2)

    # ---- page side ----

    def drain(self):
        with self.lock:
            while True:
                try:
                    self.recent.appendleft(self.inbox.get_nowait())
                except queue.Empty:
                    return

    def snapshot(self):
        self.drain()
        now = time.time()
        sec = int(now)
        with self.lock:
            counts = dict(self.tx_buckets)
            history = [counts.get(s, 0) for s in range(sec - HISTORY_S + 1, sec + 1)]  # oldest first
            self._trim_alert_times(now)
            return {
                "tx_total": self.tx_total,
                "tx_per_sec": sum(history[-RATE_WINDOW_S:]) / RATE_WINDOW_S,
                "tx_history": history,
                "alert_total": self.alert_total,
                "active_alerts": len(self.alert_times),
                "by_rule": dict(self.by_rule),
                "alerts": list(self.recent),
                "rejected_alerts": self.rejected,
                "error": self.error,
            }
