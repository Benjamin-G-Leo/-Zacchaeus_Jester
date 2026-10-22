"""Admin fraud-alerts dashboard. No web framework, just the stdlib.

    python -m fraud.dashboard      # then open http://localhost:8000

Two background threads read Kafka (alerts, plus a plain counter on the
transaction topic for throughput). The page polls /api/state once a second.
"""
import json
import threading
import time
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaConsumer

from .common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_TX, loads

PORT = 8000
INDEX = Path(__file__).with_name("static") / "index.html"

lock = threading.Lock()
alerts = deque(maxlen=200)
by_rule = Counter()
tx_total = 0
tx_times = deque()  # wall-clock arrival of recent tx, for the rate
alert_total = 0


def watch_alerts():
    global alert_total
    consumer = KafkaConsumer(
        TOPIC_ALERTS, bootstrap_servers=BOOTSTRAP, value_deserializer=loads, auto_offset_reset="latest"
    )
    for rec in consumer:
        with lock:
            alerts.appendleft(rec.value)
            by_rule[rec.value["rule"]] += 1
            alert_total += 1


def watch_transactions():
    global tx_total
    consumer = KafkaConsumer(TOPIC_TX, bootstrap_servers=BOOTSTRAP, auto_offset_reset="latest")
    for _ in consumer:  # only counting, so skip JSON parsing
        now = time.time()
        with lock:
            tx_total += 1
            tx_times.append(now)


def snapshot():
    now = time.time()
    with lock:
        while tx_times and now - tx_times[0] > 10:
            tx_times.popleft()
        return {
            "tx_total": tx_total,
            "tx_per_sec": round(len(tx_times) / 10, 1),
            "alert_total": alert_total,
            "by_rule": dict(by_rule),
            "alerts": list(alerts)[:50],
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/state":
            body, kind = json.dumps(snapshot()).encode(), "application/json"
        elif self.path in ("/", "/index.html"):
            body, kind = INDEX.read_bytes(), "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    for fn in (watch_alerts, watch_transactions):
        threading.Thread(target=fn, daemon=True).start()
    print(f"dashboard at http://localhost:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
