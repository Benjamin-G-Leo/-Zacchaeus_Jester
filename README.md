# Fraud and Anomaly Detection Pipeline

A streaming pipeline: mock transactions go into Kafka, a detector flags suspicious ones in real time using per-user state in Redis, and a dashboard shows the alerts on a live map.

```
producer --> Kafka "transactions" --> detector <--> Redis (per-user state)
                                          |
                                          v
              browser <-- dashboard <-- Kafka "fraud_alerts"
```

## Run it

Needs Docker and Python 3.

```bash
docker compose up -d                       # Kafka (KRaft, single node) and Redis
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# create topics (3 partitions each)
for t in transactions fraud_alerts; do
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done

.venv/bin/python -m fraud.detector         # terminal 1 (run more copies to scale out)
.venv/bin/python -m fraud.dashboard        # terminal 2, open http://localhost:8000
.venv/bin/python -m fraud.producer --rate 100   # terminal 3
```

Tests: `.venv/bin/python -m pytest` (Redis tests skip if Redis is not running).

## Fraud rules (`fraud/rules.py`)

All rules use event time and per-user state. Kafka keys messages by `user_id`, so one user's events stay in order on one partition.

| Rule | Fires when |
|---|---|
| Impossible travel | Two swipes 500+ km apart that would need over 900 km/h |
| Velocity | 6+ transactions inside a 10 second sliding window |
| Amount spike | Amount is over mean + 4 sigma and over 5x the user's own average (needs 5 prior samples) |

Each rule has a 30 second per-user cooldown so one burst gives one alert.

## State in Redis (`fraud/state.py`)

Per user:

- `fraud:user:<id>:state`: hash with last city and time, running amount mean and variance, cooldowns
- `fraud:user:<id>:window`: sorted set, score is event time, member is `tx_id`. This is the sliding window. Old entries are trimmed on every transaction, so each lookup stays O(log n) on a tiny set.

Two round trips per transaction (a pipeline to slide the window and load state, one to write back). Keys expire after 24 hours of inactivity. Set `STATE_BACKEND=memory` to run without Redis.

## Why Redis: the restart test

`python -m scripts.restart_test` plants three frauds, each split in two halves, and restarts the detector between the halves. A control run with no restart shows the test is fair.

| Fraud | In-memory state, after restart | Redis state, after restart |
|---|---|---|
| Impossible travel | missed | caught |
| Velocity | missed | caught |
| Amount spike | missed | caught |

With memory, one restart lost 3 of 3 frauds. With Redis, 0 of 3. Redis also lets several detectors share state and survives a Redis restart (AOF on). Trade-off: a network round trip per transaction instead of a dict lookup.

## Design notes

- Detector pulls bounded batches (`max_poll_records=500`) and commits offsets only after alerts are flushed, so a crash replays instead of losing data.
- A replayed message is not double counted in the velocity window (the sorted set member is `tx_id`). The running amount average is not replay safe, so a replay can nudge it slightly.
- Producer `send()` blocks up to 30s when the broker is slow, so load slows the producer instead of dropping data.
- Scale out: start more detector copies. They share a consumer group and split the 3 partitions.
- `tests/test_state.py` checks the Redis store gives the same alerts as the in-memory store on random traffic.

## Dashboard

Plain Python standard library server plus one HTML page (Leaflet map, OpenStreetMap tiles, so the map needs internet). New impossible-travel alerts flash a red banner, pulse on the map, and flash in the table. Reduced-motion settings turn the animation off.

## Status

- Done: Kafka and Redis compose, producer (5% impossible travel), detector with Redis state, three rules, 14 unit tests, live dashboard with map, restart test.
- Known quirks:
  - The producer fakes time by stamping the second travel swipe 1 to 3 minutes in the future. A user picked twice within that window can produce a travel alert that looks odd ("in 0.6 min"). About 3.7% of transactions get a travel alert against 5% of events planted.
  - The amount rule needs about a minute of warmup per user at default rates.
  - Map location is the city only. Single Kafka broker, no auth.
- Not done: load test beyond 100 tx/s, Docker Compose services for the Python parts, a Streamlit version of the dashboard.
