# Real-time Fraud Detection

A small system that watches a stream of (fake) card purchases, spots the suspicious ones within a moment, and shows them on a live web dashboard.

I built it to practice how real-time systems work: **Kafka** for moving messages, **Redis** for remembering things, **Python** for the logic, and **Streamlit** for the dashboard.

## What is going on, in plain words

Imagine a bank watching card purchases as they happen.

1. A **producer** makes up purchases, about 100 a second. A few percent are planted fraud, so there is something to catch.
2. **Kafka** carries them. Think of it as a post office with labelled mailboxes (called *topics*). Purchases go into the `transactions` mailbox.
3. The **detector** picks up each purchase and asks: does this look wrong *for this customer*? To answer, it has to remember things about each customer, like where they last paid and what they usually spend. It keeps those notes in **Redis**, which is a very fast notebook.
4. When something looks wrong, the detector drops an **alert** into a second mailbox, `fraud_alerts`.
5. The **dashboard** reads the alerts and shows them: counters, a map, and lists you can click.

```
producer ──▶ Kafka "transactions" ──▶ detector ──▶ Kafka "fraud_alerts" ──▶ dashboard ──▶ your browser
                                         │
                                         └── Redis (notes about each customer)
```

## The three things it catches

| Alert | What it means | It is flagged when |
|---|---|---|
| **Impossible travel** | The same card is used in two cities too far apart to have travelled between. | The cities are 500+ km apart and getting there would need more than 900 km/h (faster than a plane). Example: Mumbai, then New York five minutes later. |
| **Velocity** | Lots of purchases all at once. It looks like a stolen card being used in a hurry. | 6 or more purchases within 10 seconds. |
| **Amount spike** | One purchase far bigger than that customer's normal. | More than 5 times their average, and well above their usual ups and downs, once we have seen 5 of their purchases. Example: $800 from someone who usually spends $40. |

A customer gets at most one alert per type every 30 seconds, so one burst does not flood the screen.

## What you see on the dashboard

- **Numbers at the top:** total purchases, purchases per second, alerts in the last minute, and a count for each alert type.
- **A strip under the title** that turns red when a new impossible-travel alert arrives.
- **A world map** with a dot for each alert and an arc for each impossible trip. It needs internet, because the map shapes load from the web.
- **A legend** that explains each alert type.
- **Three lists**, one per alert type. Click a row to open its details: why it was flagged, the numbers behind it, a small map, and other recent alerts for the same customer.
- The lists refresh every 10 seconds, and they stand still while you point at them or press **Pause**, so a row never moves just as you click it.

## Run it yourself

You need **Docker Desktop** and **Python 3** (built and tested with Python 3.15). Run everything from this folder.

**1. Start Kafka and Redis**

```bash
docker compose up -d
```

**2. Install the Python packages**

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

**3. Create the Kafka mailboxes** (wait a few seconds after step 1 so Kafka is ready)

```bash
for t in transactions fraud_alerts transactions_dlq; do
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done
```

**4. Start the three programs**, each in its own terminal. Start the detector before the producer. The first time, it only sees purchases sent after it starts.

```bash
.venv/bin/python -m fraud.detector            # terminal 1: finds the fraud
.venv/bin/streamlit run dashboard/app.py      # terminal 2: the dashboard
.venv/bin/python -m fraud.producer --rate 100 # terminal 3: makes fake purchases
```

**5. Open http://localhost:8501**

To stop, press Ctrl+C in each terminal, then run `docker compose down`.

**Handy to know**

- The producer has options: `--rate` (purchases per second), `--duration` (seconds, 0 means forever), `--travel-rate` and `--fraud-rate` (how much fraud to plant). A lower `--rate`, like 20, gives a calmer screen.
- The dashboard says **"Connected, no transactions"** when the producer is not running.
- Amount spikes take a while to show up, because each customer needs 5 purchases first. At 100 a second that is about a minute. At 20 a second it is several minutes.
- Every new producer run gives the customers new home cities. If you run it again within 6 hours, some customers will seem to jump between cities. Clear the old notes first: `docker exec fraud-redis redis-cli flushdb`.

## Why Redis? A test that proves it

The detector has to remember each customer. If it kept those notes only inside the program, a restart would wipe them and it would miss fraud that straddles the restart.

`python -m scripts.restart_test` checks this. It plants three frauds, each split in two halves, and restarts the detector between the halves. (Stop your own detector first. The test refuses to run next to another one.)

| Fraud | Notes kept in the program | Notes kept in Redis |
|---|---|---|
| Impossible travel | missed | caught |
| Velocity | missed | caught |
| Amount spike | missed | caught |

With Redis, restarting loses nothing. To see the difference yourself, start the detector with the notes kept in the program instead: `STATE_BACKEND=memory .venv/bin/python -m fraud.detector`.

## Where things are

| File | What it does |
|---|---|
| `docker-compose.yml` | Starts Kafka and Redis. |
| `fraud/producer.py` | Makes the fake purchases and plants fraud. |
| `fraud/detector.py` | Reads purchases, runs the rules, sends alerts. |
| `fraud/rules.py` | The three fraud rules. This is the brain. |
| `fraud/state.py` | Remembers each customer, in Redis (or in memory). |
| `fraud/collector.py` | Hands alerts to the dashboard. |
| `fraud/retry.py` | Waits and tries again when Kafka or Redis is not ready yet. |
| `fraud/common.py` | Shared settings, the list of 10 cities, and the distance formula. |
| `dashboard/app.py` | The web page. |
| `scripts/restart_test.py` | The restart test above. |
| `tests/` | Automatic checks (99 of them). |

## What keeps it from falling over

- **Bad messages are set aside, not fatal.** Anything the detector cannot use (broken JSON, a missing field, an unknown city, a blank message) goes to a "dead letter" mailbox, `transactions_dlq`, and the detector carries on.
- **It waits for its tools.** If you start it before Kafka or Redis is ready, it retries with growing pauses (1 second, 2, 4 and so on) instead of crashing.
- **It shuts down cleanly,** so a restart is quick.
- **Customer notes expire** after 6 hours without activity, so Redis does not fill up.
- **The dashboard ignores broken alerts** and escapes any text it shows.
- **Everything is private to your computer.** Kafka, Redis and the dashboard only listen on `localhost`. None of them has a password, so do not expose them.

## Run the tests

```bash
.venv/bin/python -m pytest
```

The tests that need Redis are skipped if Redis is not running.

## Limits worth knowing

- It is a demo: one Kafka server, no login, and all data is fake.
- To make a "later" swipe without waiting, the producer stamps the second swipe of an impossible trip 1 to 3 minutes into the future. Occasionally that gives an odd-looking alert.
- If the detector crashes at exactly the wrong moment (after saving a customer's notes but before sending the alert), that one alert can be lost.
- A swipe that arrives late (older than the customer's latest one) is not checked for impossible travel.
- The map shows the city only, not the exact spot.
- The dashboard only counts alerts that arrive while it is open.
- I have not load-tested it beyond about 100 purchases a second.

## Under the hood (for the curious)

- **Customers stay in order.** Purchases are keyed by customer, so one customer's purchases always reach the same detector in order. You can run several detectors at once and Kafka splits the work between them.
- **Redis layout.** Per customer: a hash with the last city and time, the running average spend and the cooldowns, plus a sorted set of recent purchase times that acts as the 10-second window. Old entries are trimmed on every purchase.
- **Nothing is lost on a crash.** The detector confirms its position in Kafka only after its alerts are sent, so a crash replays messages instead of dropping them. A replayed purchase is not double-counted in the velocity window.
- **Distance** uses the Haversine formula, checked in `tests/test_geo.py`.
- **The dashboard** reads Kafka on one background thread and passes alerts to the page through a queue. The page redraws once a second. A tiny script freezes the lists while you point at them.
- **Both ways of keeping notes are tested to behave the same** (`tests/test_state.py`), including when purchases arrive out of order.
