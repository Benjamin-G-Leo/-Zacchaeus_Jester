"""Does the detector lose fraud when it restarts?

    python -m scripts.restart_test

Runs the same planted fraud twice against a real detector process:
  control: detector stays up the whole time
  restart: detector is stopped and started between the two halves of each fraud

Each fraud is split in two, so catching it needs memory of the first half:
  travel:   Mumbai swipe, then New York swipe 5 simulated minutes later
  velocity: 3 swipes, then 3 more inside the same 10s window
  amount:   10 normal purchases, then one 20x purchase
"""
import os
import signal
import subprocess
import sys
import threading
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from fraud.common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_TX, dumps, loads
from fraud.retry import connect_kafka

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRAUDS = ("impossible_travel", "velocity", "amount_spike")


def start_detector(backend):
    p = subprocess.Popen(
        [sys.executable, "-u", "-m", "fraud.detector"],
        cwd=ROOT,
        env={**os.environ, "STATE_BACKEND": backend},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        # a shell that started us in the background may have SIGINT ignored; undo that
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
    )
    ready = threading.Event()

    def pump():  # keep draining so the pipe never fills, and watch for the ready line
        for line in p.stdout:
            if "detector ready" in line:
                ready.set()

    threading.Thread(target=pump, daemon=True).start()
    # Wait until it truly owns partitions. A fixed sleep is flaky: after a hard kill,
    # Kafka keeps the old member's partitions for about 10 seconds.
    if not ready.wait(90):
        p.kill()
        sys.exit("detector did not become ready within 90s")
    return p


def stop_detector(p):
    p.send_signal(signal.SIGINT)  # clean exit, leaves the group right away
    p.wait(timeout=20)


def send(producer, user, city, amount, ts):
    producer.send(
        TOPIC_TX,
        key=user,
        value={
            "tx_id": uuid.uuid4().hex[:12],
            "user_id": user,
            "amount": amount,
            "city": city,
            "merchant": "test",
            "ts": ts,
        },
    )


def first_halves(producer, users, t):
    send(producer, users["impossible_travel"], "Mumbai", 40.0, t)
    for i in range(3):
        send(producer, users["velocity"], "Mumbai", 10.0, t + i)
    for i in range(10):
        send(producer, users["amount_spike"], "Mumbai", 40.0 + i % 3, t - 2000 + i * 100)
    producer.flush()


def second_halves(producer, users, t):
    send(producer, users["impossible_travel"], "New York", 40.0, t + 300)
    for i in range(3, 6):
        send(producer, users["velocity"], "Mumbai", 10.0, t + i)
    send(producer, users["amount_spike"], "Mumbai", 800.0, t)
    producer.flush()


def run(label, restart, backend):
    run_id = uuid.uuid4().hex[:6]
    users = {f: f"{label}-{f[:3]}-{run_id}" for f in FRAUDS}
    by_user = {u: f for f, u in users.items()}

    alerts = connect_kafka(KafkaConsumer, bootstrap_servers=BOOTSTRAP, value_deserializer=loads)
    parts = [TopicPartition(TOPIC_ALERTS, p) for p in alerts.partitions_for_topic(TOPIC_ALERTS)]
    alerts.assign(parts)
    alerts.seek_to_end(*parts)

    producer = connect_kafka(
        KafkaProducer, bootstrap_servers=BOOTSTRAP, value_serializer=dumps, key_serializer=str.encode
    )
    detector = start_detector(backend)
    t = time.time()

    first_halves(producer, users, t)
    time.sleep(4)  # detector reads and commits the first halves

    if restart:
        print(f"  [{label}] stopping detector, starting a fresh one...")
        stop_detector(detector)
        detector = start_detector(backend)

    second_halves(producer, users, t)
    caught = set()
    deadline = time.time() + 8
    while time.time() < deadline:
        for recs in alerts.poll(timeout_ms=500).values():
            for r in recs:
                if r.value["user_id"] in by_user:
                    caught.add(by_user[r.value["user_id"]])

    stop_detector(detector)
    alerts.close()
    producer.close()
    return caught


def other_detectors():
    out = subprocess.run(["pgrep", "-f", "fraud.detector"], capture_output=True, text=True).stdout
    return [p for p in out.split() if p != str(os.getpid())]


def main():
    # A detector already running joins the same consumer group, takes some of the
    # partitions, and quietly catches frauds with its own state. That ruins the test.
    others = other_detectors()
    if others:
        sys.exit(f"another detector is running (pid {', '.join(others)}) and shares the consumer group. "
                 "Stop it first, e.g. kill " + others[0])
    backends = sys.argv[1].split(",") if len(sys.argv) > 1 else ["memory", "redis"]
    results = {}
    for backend in backends:
        for label, restart in (("control", False), ("restart", True)):
            print(f"running {backend} / {label} ...")
            results[backend, label] = run(label, restart, backend)

    print()
    print(f"{'fraud':<20}" + "".join(f"{b + ' (restart)':<20}" for b in backends))
    for f in FRAUDS:
        print(f"{f:<20}" + "".join(
            f"{'caught' if f in results[b, 'restart'] else 'MISSED':<20}" for b in backends))
    print()
    bad = False
    for b in backends:
        base, after = results[b, "control"], results[b, "restart"]
        lost = base - after
        print(f"{b}: caught {len(after)} of {len(FRAUDS)} after restart "
              f"(control without restart caught {len(base)})")
        bad |= bool(lost) or len(base) < len(FRAUDS)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
