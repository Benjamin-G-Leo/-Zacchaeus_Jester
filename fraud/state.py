"""Where per-user fraud state lives: Python memory or Redis.

Both stores expose `process(tx) -> alerts`. The detector doesn't care which.

Redis layout, per user:
  fraud:user:<id>:state   hash  last location/time, running amount stats, cooldowns
  fraud:user:<id>:window  zset  score = event time, member = tx_id (sliding window)

Two round trips per transaction (one pipeline to read and slide the window,
one to write back). Every command is O(1) or O(log n) on a tiny set.
"""
import os

from urllib.parse import urlparse

from .common import REDIS_PASSWORD, REDIS_URL
from .retry import retry_with_backoff
from .rules import VELOCITY_WINDOW_S, UserState, evaluate

# Every write resets this, so a user's keys vanish 6 hours after their last
# transaction and Redis can't fill up with users who never come back. The state
# is a hash and the window a sorted set, and SETEX only works on strings, so the
# TTL is set with EXPIRE in the same pipeline as the write.
TTL_S = 6 * 3600


class MemoryStore:
    name = "memory"

    def __init__(self):
        self.users = {}

    def process(self, tx):
        return self.users.setdefault(tx["user_id"], UserState()).process(tx)


class RedisStore:
    name = "redis"

    def __init__(self, client):
        self.r = client

    def process(self, tx):
        uid, ts = tx["user_id"], tx["ts"]
        skey = f"fraud:user:{uid}:state"
        wkey = f"fraud:user:{uid}:window"

        # Slide the window, add this tx, count it, load state: one round trip.
        # The member is tx_id, so a replayed message doesn't count twice.
        # ZCOUNT, not ZCARD: only swipes in the window that ends at this one count, so a
        # future-dated entry cannot inflate the total (same rule as the in-memory store).
        pipe = self.r.pipeline()
        pipe.hgetall(skey)
        pipe.zremrangebyscore(wkey, "-inf", f"({ts - VELOCITY_WINDOW_S}")
        pipe.zadd(wkey, {tx["tx_id"]: ts})
        pipe.zcount(wkey, ts - VELOCITY_WINDOW_S, ts)
        pipe.expire(wkey, TTL_S)
        fields, _, _, count, _ = pipe.execute()

        state = UserState.from_fields(fields)
        alerts = evaluate(state, tx, count)

        pipe = self.r.pipeline()
        pipe.hset(skey, mapping=state.to_fields())
        pipe.expire(skey, TTL_S)
        pipe.execute()
        return alerts


def redis_client(db=None):
    """A Redis client using REDIS_URL and REDIS_PASSWORD. `db` overrides the database in the URL."""
    import redis

    u = urlparse(REDIS_URL)
    return redis.Redis(
        host=u.hostname or "localhost",
        port=u.port or 6379,
        db=db if db is not None else int((u.path or "/0").lstrip("/") or 0),
        password=REDIS_PASSWORD or u.password or None,
        decode_responses=True,
    )


def make_store():
    """STATE_BACKEND=redis (default) or memory."""
    if os.environ.get("STATE_BACKEND", "redis") == "memory":
        return MemoryStore()
    import redis

    client = redis_client()
    retry_with_backoff(  # Redis may still be starting under Docker
        client.ping,
        "Redis",
        retry_on=(redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError),
        # AuthenticationError is a kind of ConnectionError, but a wrong password never fixes itself
        fatal=(redis.exceptions.AuthenticationError,),
    )
    return RedisStore(client)
