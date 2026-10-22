"""Consume transactions, run fraud rules, publish alerts.

    python -m fraud.detector

Run several copies to scale out: they share the consumer group and Kafka
splits the 3 partitions between them. Messages are keyed by user_id, so a
given user's events always land on the same detector.
"""
import time

from kafka import KafkaConsumer, KafkaProducer

from .common import BOOTSTRAP, TOPIC_ALERTS, TOPIC_TX, dumps, loads
from .retry import connect_kafka
from .state import make_store


def main():
    consumer = connect_kafka(
        KafkaConsumer,
        TOPIC_TX,
        bootstrap_servers=BOOTSTRAP,
        group_id="fraud-detector",
        value_deserializer=loads,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # Backpressure: pull a bounded batch, only fetch more when done.
        max_poll_records=500,
    )
    producer = connect_kafka(
        KafkaProducer, bootstrap_servers=BOOTSTRAP, value_serializer=dumps, key_serializer=str.encode, linger_ms=10
    )

    store = make_store()
    seen = flagged = 0
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
                    t = rec.value
                    seen += 1
                    for alert in store.process(t):
                        producer.send(TOPIC_ALERTS, key=t["user_id"], value=alert)
                        flagged += 1
            if batch:
                producer.flush()  # alerts are out before we mark the batch done
                consumer.commit()
            if time.time() - last_report >= 5:
                print(f"processed {seen} tx, {flagged} alerts")
                last_report = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
