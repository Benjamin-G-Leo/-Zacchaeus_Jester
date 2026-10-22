import pytest
from kafka.errors import KafkaConfigurationError, KafkaTimeoutError

from fraud.retry import KAFKA_RETRY_ON, retry_with_backoff


class Fake:
    """A clock that only moves when sleep() is called, so tests run instantly."""

    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s

    def clock(self):
        return self.t


def flaky(failures, exc=OSError):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise exc("not up yet")
        return "ok"

    return fn, calls


def run(fn, fake, **kw):
    kw.setdefault("retry_on", (OSError,))
    return retry_with_backoff(fn, "thing", jitter=0, sleep=fake.sleep, clock=fake.clock, **kw)


def test_waits_double_each_time_then_succeeds():
    fake = Fake()
    fn, calls = flaky(3)
    assert run(fn, fake) == "ok"
    assert fake.sleeps == [1, 2, 4]
    assert calls["n"] == 4


def test_wait_is_capped():
    fake = Fake()
    fn, _ = flaky(5)
    run(fn, fake, max_delay_s=5, deadline_s=None)
    assert fake.sleeps == [1, 2, 4, 5, 5]


def test_gives_up_at_deadline_and_raises_last_error():
    fake = Fake()
    fn, _ = flaky(10**6)
    with pytest.raises(OSError):
        run(fn, fake, deadline_s=10)
    assert fake.sleeps == [1, 2, 4]  # next wait of 8 would pass the 10s deadline


def test_no_deadline_keeps_going():
    fake = Fake()
    fn, calls = flaky(50)
    assert run(fn, fake, deadline_s=None) == "ok"
    assert calls["n"] == 51


def test_unrelated_errors_are_not_retried():
    fake = Fake()
    fn, calls = flaky(1, exc=ValueError)
    with pytest.raises(ValueError):
        run(fn, fake)
    assert calls["n"] == 1 and fake.sleeps == []


def test_fatal_errors_are_not_retried():
    fake = Fake()
    fn, calls = flaky(1, exc=KafkaConfigurationError)
    with pytest.raises(KafkaConfigurationError):
        run(fn, fake, retry_on=KAFKA_RETRY_ON, fatal=(KafkaConfigurationError,))
    assert calls["n"] == 1


def test_jitter_stays_within_bounds():
    fake = Fake()
    fn, _ = flaky(4)
    retry_with_backoff(fn, "thing", retry_on=(OSError,), jitter=0.2, sleep=fake.sleep, clock=fake.clock)
    for got, base in zip(fake.sleeps, [1, 2, 4, 8]):
        assert 0.8 * base <= got <= 1.2 * base


def test_kafka_no_broker_error_is_retried():
    # what kafka-python 3.x raises when Kafka is still starting
    assert issubclass(KafkaTimeoutError, KAFKA_RETRY_ON)
