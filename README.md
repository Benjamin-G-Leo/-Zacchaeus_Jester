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
for t in transactions fraud_alerts transactions_dlq; do
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done

.venv/bin/python -m fraud.detector         # terminal 1 (run more copies to scale out)
.venv/bin/streamlit run dashboard/app.py   # terminal 2, open http://localhost:8501
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

Two round trips per transaction (a pipeline to slide the window and load state, one to write back). Every write resets a 6 hour expiry on both keys, so users who stop transacting disappear from Redis after 6 hours and memory stays bounded. (`SETEX` only works on string keys. The state is a hash and the window a sorted set, so the expiry is set with `EXPIRE` in the same pipeline as the write.) Set `STATE_BACKEND=memory` to run without Redis.

## Why Redis: the restart test

`python -m scripts.restart_test` plants three frauds, each split in two halves, and restarts the detector between the halves. A control run with no restart shows the test is fair.

| Fraud | In-memory state, after restart | Redis state, after restart |
|---|---|---|
| Impossible travel | missed | caught |
| Velocity | missed | caught |
| Amount spike | missed | caught |

With memory, one restart lost 3 of 3 frauds. With Redis, 0 of 3. The test waits for the detector to say it owns its partitions before sending traffic, and refuses to run if another detector is already in the consumer group (that one would quietly catch frauds and spoil the result). Redis also lets several detectors share state and survives a Redis restart (AOF on). Trade-off: a network round trip per transaction instead of a dict lookup.

## Design notes

- Detector pulls bounded batches (`max_poll_records=500`) and commits offsets only after alerts are flushed, so a crash replays instead of losing data.
- A replayed message is not double counted in the velocity window (the sorted set member is `tx_id`). The running amount average is not replay safe, so a replay can nudge it slightly.
- Producer `send()` blocks up to 30s when the broker is slow, so load slows the producer instead of dropping data.
- Scale out: start more detector copies. They share a consumer group and split the 3 partitions.
- `tests/test_state.py` checks the Redis store gives the same alerts as the in-memory store on random traffic, and that keys get and keep a 6 hour expiry.
- Startup retry: producer, detector, dashboard and the restart test connect to Kafka and Redis through `fraud/retry.py`. It waits 1s, 2s, 4s and so on (capped at 30s, with jitter) and gives up after 3 minutes, so starting before Docker is ready no longer crashes. kafka-python 3.x raises `KafkaTimeoutError` when no broker answers (there is no `NoBrokersAvailable`), so the retry catches `KafkaError`. A bad config value is never retried. The dashboard retries forever.
- Bad input cannot take the pipeline down. The detector checks every transaction (`validate` in `fraud/rules.py`) before it touches state: not JSON, missing fields, an unknown city, NaN or negative amounts all go to the `transactions_dlq` topic and are skipped. Before this, one message with an unknown city crashed the detector, and because offsets are committed after a batch it crashed again on every restart. Infrastructure errors (Redis down) are deliberately not caught, so the detector stops and replays instead of dropping data. Stored state with an unknown city is treated as "no previous location" and heals on the next transaction.
- The dashboard collector validates every alert (`parse_alert` in `fraud/collector.py`). Before, one alert with a missing field raised a `KeyError` on every redraw and froze the page for everyone. Text from the wire is HTML-escaped in the detail popup and stripped of Markdown in the row labels.
- The detector treats SIGTERM like Ctrl+C, so `docker stop` and `kill` make it leave the consumer group at once. Before, Kafka held its partitions for about 10 seconds and restarts stalled. Failed alert deliveries are logged, since `flush()` does not raise for them.
- Velocity counts only swipes in the 10 second window that ends at the current one, in both stores. Before, a future-dated swipe inflated the count, and the in-memory store kept stale entries hidden behind it.
- Kafka, Redis and the dashboard bind to `127.0.0.1` only. None of them has a login.
- Distance uses the Haversine formula (`fraud/common.py`), checked in `tests/test_geo.py` against known city distances, the date line, and antipodal points.

## Dashboard

Streamlit app (`dashboard/app.py`) styled like a modern SaaS tool: near-black background, thin borders, Inter for text and Geist Mono for numbers (loaded from Google Fonts, with system fallbacks).

How data gets to the page:

- Streamlit re-runs the script constantly, so no Kafka code lives in it. One daemon thread in `fraud/collector.py` owns a single `KafkaConsumer` subscribed to `transactions` and `fraud_alerts`.
- Alerts cross to the page through a thread-safe `queue.Queue` (bounded; if no page is open it drops the oldest). Plain transactions are only counted, per second, which feeds the throughput sparkline.
- The page draws one frame, runs `time.sleep(1.0)`, then `st.rerun()`. One collector serves every browser tab.

What is on screen:

- Metric badges with a small status dot: transactions, throughput with a 60 second sparkline, active anomalies (alerts in the last 60 seconds, with a pulsing red dot while any exist), and a count per rule.
- A bordered world map: a dot per alert (larger and outlined while new) and curved arcs for impossible travel. Plotly loads country shapes from cdn.plot.ly, so the map needs internet. It redraws every 3 seconds, not every frame, so it does not flicker.
- Live alerts split into three lists, one per rule (impossible travel, velocity, amount spike), each with its own running count. Every row is a button: a green dot marks a new alert, and clicking a row opens a detail popup.
- The detail popup shows a plain-English reason, the numbers behind the rule (distance, time and implied speed for travel; count and window for velocity; typical purchase, multiple and threshold for amount spikes), a small map of the cities, other recent alerts for the same user, and the raw event.
- A thin red banner for 6 seconds when a new impossible-travel alert arrives. Reduced-motion settings turn the animations off.

The live loop pauses while a popup is open, which also freezes the lists so you can read. Closing it with Close or the X resumes updates. The one-second wait is ten 0.1 second sleeps with a trivial Streamlit call between them, because Streamlit only notices a click at its next call. That makes a click open the popup in about 0.1 seconds instead of up to a second.

Because of the sleep and rerun loop, the page keeps one Python script running per open browser tab, and CSS hides Streamlit's "running" indicator and its fade on old elements. Redis keeps user state for 6 hours, so after changing the producer seed run `docker exec fraud-redis redis-cli flushdb` first, or users will seem to teleport between cities.

## Status

- Done: Kafka and Redis compose, producer (5% impossible travel), detector with Redis state, three rules, 95 tests, Streamlit dashboard with map, restart test, startup retry, 6 hour state expiry.
- Known quirks:
  - The producer fakes time by stamping the second travel swipe 1 to 3 minutes in the future. A user picked twice within that window can produce a travel alert that looks odd ("in 0.6 min"). About 3.7% of transactions get a travel alert against 5% of events planted.
  - The amount rule needs about a minute of warmup per user at default rates.
  - Map location is the city only. Single Kafka broker, no auth.
  - The dashboard only counts alerts that arrive while it is running (it reads from the latest offset).
- Known limits (found in review, not fixed):
  - If the detector crashes after writing state to Redis but before its alerts are flushed, the replay sees the cooldown already recorded and does not send the alert again, so that alert is lost. Fixing it properly needs a transactional outbox.
  - A swipe that arrives late (older than the user's latest one) is not checked for impossible travel.
  - State is read and written in two steps, so two detectors handling the same user at once (briefly possible during a rebalance) can lose an update.
  - A user whose spending legitimately moves to a new level keeps raising spike alerts, because spikes are kept out of the baseline.
- Not done: load test beyond 100 tx/s, Docker Compose services for the Python parts.
