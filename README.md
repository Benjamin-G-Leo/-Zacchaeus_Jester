# Fraud and Anomaly Detection Pipeline

A streaming pipeline: mock transactions go into Kafka, a detector flags suspicious ones in real time, and a dashboard shows the alerts.

```
producer.py --> Kafka "transactions" --> detector.py --> Kafka "fraud-alerts" --> dashboard.py --> browser
```

## Run it

Needs Docker, Python 3.

```bash
docker compose up -d                       # Kafka (KRaft, single node)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# create topics (3 partitions each)
for t in transactions fraud-alerts; do
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done

.venv/bin/python -m fraud.detector         # terminal 1 (run more copies to scale out)
.venv/bin/python -m fraud.dashboard        # terminal 2, open http://localhost:8000
.venv/bin/python -m fraud.producer --rate 100   # terminal 3
```

Tests: `.venv/bin/python -m pytest`

## Fraud rules (`fraud/rules.py`)

All rules use event time and per-user state. Kafka keys messages by `user_id`, so one user's events stay in order on one partition.

| Rule | Fires when |
|---|---|
| Impossible travel | Two swipes 500+ km apart that would need over 900 km/h |
| Velocity | 6+ transactions inside a 10 second sliding window |
| Amount spike | Amount is over mean + 4 sigma and over 5x the user's own average (needs 5 prior samples) |

Each rule has a 30 second per-user cooldown so one burst gives one alert.

## Design notes

- Detector pulls bounded batches (`max_poll_records=500`) and commits offsets only after alerts are flushed, so a crash replays instead of losing data.
- Producer `send()` blocks up to 30s when the broker is slow, so load slows the producer rather than dropping data.
- Scale out: start more detector copies. They share a consumer group and split the 3 partitions.

## Status

- Done: Kafka compose, producer, detector, rules plus 9 unit tests, live dashboard. Verified end to end at about 100 tx/s.
- Tuning note: the first run used 200 users, so normal traffic tripped the velocity rule. Fixed with 1000 users and a 10 second window.
- Known limits: single Kafka broker; detector state is in memory, so a restart forgets user history; the amount rule needs about a minute of warmup per user at default rates.
- Not done: persist state (Redis or Kafka Streams), alert deduping across detector restarts, load test beyond 100 tx/s, Docker Compose services for the Python parts.
