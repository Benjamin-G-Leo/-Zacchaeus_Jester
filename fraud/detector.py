"""Consume transactions, run fraud rules, publish alerts.

    python -m fraud.detector

Run several copies to scale out: they share the consumer group and Kafka
splits the 3 partitions between them. Messages are keyed by user_id, so a
given user's events always land on the same detector.

A message the detector cannot use (not JSON, missing fields, unknown city, NaN
amount, ...) goes to the `transactions_dlq` topic and is skipped. Without that,
one bad message would crash the loop, and because offsets are only committed
after a batch it would crash again on every restart.
"""
import signal
import sys
import time

from kafka import KafkaConsumer, KafkaProducer

from .common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_DLQ, TOPIC_TX, dumps, loads
from .retry import connect_kafka
from .rules import validate
from .state import make_store


def handle(raw, store):
    """One raw Kafka message -> (alerts, reason). reason is None when the message was fine.

    Only bad *data* is rejected here. Infrastructure errors (Redis down, ...) are not
    caught on purpose: the detector should stop and replay, not drop messages.
    """
    if raw is None:  # a Kafka "tombstone": a record with no value at all
        return [], "empty message"
    try:
        tx = loads(raw)
    except ValueError as e:  # bad JSON or bad UTF-8
        return [], f"not JSON: {e}"
    reason = validate(tx)
    if reason:
        return [], reason
    return store.process(tx), None


def on_send_error(exc):
    # producer.flush() does not raise for a message that failed to deliver, so say so here
    print(f"[delivery failed] {exc!r}", file=sys.stderr, flush=True)


def _stop(signum, frame):
    # `docker stop` and `kill` send SIGTERM. Treat it like Ctrl+C so the consumer leaves
    # the group at once; otherwise Kafka holds its partitions for ~10s and a restart stalls.
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, _stop)
    consumer = connect_kafka(
        KafkaConsumer,
        TOPIC_TX,
        bootstrap_servers=BOOTSTRAP,
        group_id="fraud-detector",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # Backpressure: pull a bounded batch, only fetch more when done.
        max_poll_records=500,
    )
    producer = connect_kafka(
        KafkaProducer, bootstrap_servers=BOOTSTRAP, value_serializer=dumps, key_serializer=str.encode, linger_ms=10
    )

    store = make_store()
    seen = flagged = rejected = 0
    ready = False
    last_report = time.time()
    print(f"detector listening on '{TOPIC_TX}', state in {store.name}. Ctrl+C to stop.")
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not ready and consumer.assignment():
                # tests and scripts wait for this line before sending traffic
                parts = sorted(p.partition for p in consumer.assignment())
                print(f"detector ready, partitions {parts}", flush=True)
                ready = True
            for records in batch.values():
                for rec in records:
                    seen += 1
                    alerts, reason = handle(rec.value, store)
                    if reason:
                        rejected += 1
                        dead = {
                            "reason": reason,
                            "raw": (rec.value or b"").decode("utf-8", "replace")[:2000],
                            "partition": rec.partition,
                            "offset": rec.offset,
                            "ts": time.time(),
                        }
                        producer.send(TOPIC_DLQ, key="bad", value=dead).add_errback(on_send_error)
                        continue
                    for alert in alerts:
                        producer.send(TOPIC_ALERTS, key=alert["user_id"], value=alert).add_errback(on_send_error)
                        flagged += 1
            if batch:
                producer.flush()  # alerts are out before we mark the batch done
                consumer.commit()
            if time.time() - last_report >= 5:
                print(f"processed {seen} tx, {flagged} alerts, {rejected} rejected")
                last_report = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
