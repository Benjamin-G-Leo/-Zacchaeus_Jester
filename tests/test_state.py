"""The Redis store must behave exactly like the in-memory one."""
import random

import pytest

from fraud.common import CITIES
from fraud.state import MemoryStore, RedisStore

redis = pytest.importorskip("redis")


@pytest.fixture
def client():
    r = redis.Redis(db=15, decode_responses=True)  # db 15 so tests never touch real state
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.skip("Redis is not running (docker compose up -d)")
    r.flushdb()
    yield r
    r.flushdb()


def stream(seed, n=600):
    """Random but reproducible traffic: bursts, travel, spikes, out-of-order times."""
    rng = random.Random(seed)
    cities = list(CITIES)
    users = {f"u{i}": rng.choice(cities) for i in range(5)}
    t = 1_000_000.0
    for i in range(n):
        t += rng.uniform(0, 8)
        u = rng.choice(list(users))
        r = rng.random()
        city = users[u] if r < 0.85 else rng.choice(cities)
        amount = rng.uniform(20, 60) if r < 0.95 else rng.uniform(500, 900)
        jitter = rng.uniform(-3, 0) if rng.random() < 0.1 else 0  # a little out of order
        yield {"tx_id": f"t{seed}-{i}", "user_id": u, "amount": round(amount, 2), "city": city, "ts": t + jitter}


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_redis_matches_memory(client, seed):
    mem, red = MemoryStore(), RedisStore(client)
    total = 0
    for tx in stream(seed):
        a, b = mem.process(tx), red.process(tx)
        assert [x["rule"] for x in a] == [x["rule"] for x in b], tx
        total += len(a)
    assert total > 0  # the stream actually triggered rules


def test_state_survives_a_new_store_object(client):
    """The whole point: a fresh process (new store) still remembers the user."""
    tx = lambda i, city, ts: {"tx_id": f"x{i}", "user_id": "u9", "amount": 40.0, "city": city, "ts": ts}
    RedisStore(client).process(tx(1, "Mumbai", 1000))
    out = RedisStore(client).process(tx(2, "New York", 1300))
    assert [a["rule"] for a in out] == ["impossible_travel"]


def test_replayed_message_is_not_double_counted(client):
    red = RedisStore(client)
    tx = {"tx_id": "same", "user_id": "u8", "amount": 10.0, "city": "Mumbai", "ts": 5000}
    for _ in range(10):
        out = red.process(tx)
    assert out == []  # 10 replays of one tx_id is still one swipe, no velocity alert
