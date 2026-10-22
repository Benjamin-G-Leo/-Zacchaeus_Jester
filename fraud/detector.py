"""Consume transactions, run fraud rules, publish alerts.

    python -m fraud.detector

Run several copies to scale out: they share the consumer group and Kafka
splits the 3 partitions between them. Messages are keyed by user_id, so a
given user's events always land on the same detector.
"""
import time

from kafka import KafkaConsumer, KafkaProducer

from .common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_TX, dumps, loads
from .rules import UserState


def main():
    consumer = KafkaConsumer(
        TOPIC_TX,
        bootstrap_servers=BOOTSTRAP,
        group_id="fraud-detector",
        value_deserializer=loads,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # Backpressure: pull a bounded batch, only fetch more when done.
        max_poll_records=500,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP, value_serializer=dumps, key_serializer=str.encode, linger_ms=10
    )

    states = {}
    seen = flagged = 0
    last_report = time.time()
    print(f"detector listening on '{TOPIC_TX}'. Ctrl+C to stop.")
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            for records in batch.values():
                for rec in records:
                    t = rec.value
                    seen += 1
                    state = states.setdefault(t["user_id"], UserState())
                    for alert in state.process(t):
                        producer.send(TOPIC_ALERTS, key=t["user_id"], value=alert)
                        flagged += 1
            if batch:
                producer.flush()  # alerts are out before we mark the batch done
                consumer.commit()
            if time.time() - last_report >= 5:
                print(f"processed {seen} tx, {flagged} alerts, {len(states)} users tracked")
                last_report = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
