"""Pump mock transactions into Kafka.

    python -m fraud.producer --rate 50

Most transactions are normal. A few percent are planted fraud so the
detector has something to find: impossible travel, velocity bursts, and
amount spikes.
"""
import argparse
import random
import time
import uuid

from kafka import KafkaProducer

from .common import BOOTSTRAP, CITIES, TOPIC_TX, dumps
from .retry import connect_kafka


def make_users(n, rng):
    cities = list(CITIES)
    return [
        {
            "user_id": f"u{i:04d}",
            "home": rng.choice(cities),
            "typical": rng.lognormvariate(3.7, 0.6),  # ~ $40 median
        }
        for i in range(n)
    ]


def tx(user, city, amount, ts):
    return {
        "tx_id": uuid.uuid4().hex[:12],
        "user_id": user["user_id"],
        "amount": round(amount, 2),
        "city": city,
        "merchant": random.choice(["grocery", "fuel", "cafe", "online", "travel", "electronics"]),
        "ts": ts,
    }


def normal(user, ts, rng):
    amount = max(1.0, rng.gauss(user["typical"], user["typical"] * 0.25))
    return [tx(user, user["home"], amount, ts)]


def impossible_travel(user, ts, rng):
    far = rng.choice([c for c in CITIES if c != user["home"]])
    # Second swipe is stamped 1 to 3 simulated minutes later so we don't wait for real.
    pair = [
        tx(user, user["home"], user["typical"], ts),
        tx(user, far, user["typical"], ts + rng.uniform(60, 180)),
    ]
    # From now on this user shops in the new city. Otherwise their next normal swipe
    # back home would be a second, legitimate-looking impossible trip.
    user["home"] = far
    return pair


def velocity_burst(user, ts, rng):
    return [
        tx(user, user["home"], rng.uniform(5, 30), ts + i) for i in range(rng.randint(6, 9))
    ]


def amount_spike(user, ts, rng):
    return [tx(user, user["home"], user["typical"] * rng.uniform(12, 25), ts)]


OTHER_FRAUD = [velocity_burst, amount_spike]


def pick(rng, travel_rate, other_rate):
    r = rng.random()
    if r < travel_rate:
        return impossible_travel
    if r < travel_rate + other_rate:
        return rng.choice(OTHER_FRAUD)
    return normal


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=50, help="transactions per second")
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--travel-rate", type=float, default=0.05, help="share of events that are impossible travel")
    p.add_argument("--fraud-rate", type=float, default=0.02, help="share that are velocity or amount fraud")
    p.add_argument("--duration", type=float, default=0, help="seconds, 0 = forever")
    p.add_argument("--seed", type=int)
    args = p.parse_args()
    if args.rate <= 0:
        p.error("--rate must be above 0")  # it is divided into 1 second below
    if args.users < 1:
        p.error("--users must be at least 1")
    if not (0 <= args.travel_rate and 0 <= args.fraud_rate and args.travel_rate + args.fraud_rate <= 1):
        p.error("--travel-rate and --fraud-rate must be 0 or more and add up to at most 1")

    rng = random.Random(args.seed)
    users = make_users(args.users, rng)
    producer = connect_kafka(
        KafkaProducer,
        bootstrap_servers=BOOTSTRAP,
        value_serializer=dumps,
        key_serializer=str.encode,
        linger_ms=20,
        batch_size=64 * 1024,
        acks=1,
        # send() blocks (up to this long) when the broker can't take more,
        # so a slow broker slows the producer instead of losing data.
        max_block_ms=30_000,
    )

    sent = 0
    start = time.time()
    next_tick = start
    last_report = start
    print(f"producing ~{args.rate:g} tx/s to '{TOPIC_TX}' ({args.users} users). Ctrl+C to stop.")
    try:
        while args.duration == 0 or time.time() - start < args.duration:
            ts = time.time()
            user = rng.choice(users)
            make = pick(rng, args.travel_rate, args.fraud_rate)
            for t in make(user, ts, rng):
                producer.send(TOPIC_TX, key=t["user_id"], value=t)
                sent += 1
            next_tick += 1 / args.rate
            # After a stall (a blocked send), don't try to catch up all at once: that would be
            # an unplanned burst. Forget any backlog beyond one second.
            next_tick = max(next_tick, time.time() - 1.0)
            delay = next_tick - time.time()
            if delay > 0:
                time.sleep(delay)
            if time.time() - last_report >= 5:
                print(f"sent {sent} ({sent / (time.time() - start):.0f}/s)")
                last_report = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"done, sent {sent}")


if __name__ == "__main__":
    main()
