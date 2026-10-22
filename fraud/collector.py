"""Kafka to dashboard hand-off. No Streamlit in here, so it can be tested.

Streamlit re-runs the page script constantly, so nothing that reads Kafka may
live in it. Two daemon threads own the consumers. Alerts cross to the page
through a thread-safe queue.Queue; the page calls snapshot(), which drains
that queue. Counters are kept by the threads, so they stay exact even if no
page is open.
"""
import queue
import threading
import time
from collections import Counter, deque

from kafka import KafkaConsumer

from .common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_TX, loads
from .retry import connect_kafka

RATE_WINDOW_S = 10


class Collector:
    def __init__(self, start=True, inbox_max=5000, recent_max=300):
        self.inbox = queue.Queue(maxsize=inbox_max)  # reader thread -> page
        self.lock = threading.Lock()  # guards everything below
        self.recent = deque(maxlen=recent_max)  # newest first
        self.by_rule = Counter()
        self.alert_total = 0
        self.tx_total = 0
        self.tx_times = deque()
        self.error = None
        if start:
            for fn in (self._watch_alerts, self._watch_tx):
                threading.Thread(target=self._keep_running, args=(fn,), daemon=True).start()

    # ---- reader-thread side ----

    def put_alert(self, alert):
        with self.lock:
            self.by_rule[alert["rule"]] += 1
            self.alert_total += 1
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
        with self.lock:
            self.tx_total += 1
            self.tx_times.append(time.time())

    def _note_error(self, err):
        self.error = f"{type(err).__name__}: {err}"

    def _connect(self, topic, **config):
        # deadline_s=None: keep waiting for Kafka for as long as it takes
        consumer = connect_kafka(
            KafkaConsumer,
            topic,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
            deadline_s=None,
            on_retry=self._note_error,
            **config,
        )
        self.error = None
        return consumer

    def _watch_alerts(self):
        for rec in self._connect(TOPIC_ALERTS, value_deserializer=loads):
            alert = rec.value
            alert["seen_at"] = time.time()  # event time can be simulated future; show arrival time
            self.put_alert(alert)

    def _watch_tx(self):
        for _ in self._connect(TOPIC_TX):  # only counting, so skip JSON parsing
            self.count_tx()

    def _keep_running(self, fn):
        while True:  # if Kafka drops mid-stream, reconnect (connect_kafka backs off)
            try:
                fn()
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
        with self.lock:
            while self.tx_times and now - self.tx_times[0] > RATE_WINDOW_S:
                self.tx_times.popleft()
            return {
                "tx_total": self.tx_total,
                "tx_per_sec": len(self.tx_times) / RATE_WINDOW_S,
                "alert_total": self.alert_total,
                "by_rule": dict(self.by_rule),
                "alerts": list(self.recent),
                "error": self.error,
            }
