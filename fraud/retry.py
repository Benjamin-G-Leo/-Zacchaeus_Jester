"""Retry with exponential backoff, for when Docker is still starting Kafka or Redis."""
import random
import sys
import time

from kafka.errors import KafkaConfigurationError, KafkaError

# kafka-python 3.x raises KafkaTimeoutError ("Unable to bootstrap") when no broker
# answers, not NoBrokersAvailable, so catch the KafkaError base class.
KAFKA_RETRY_ON = (KafkaError, OSError)


def retry_with_backoff(
    fn,
    what,
    *,
    retry_on,
    fatal=(),
    base_s=1.0,
    factor=2.0,
    max_delay_s=30.0,
    deadline_s=180.0,
    jitter=0.2,
    on_retry=None,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Call fn() until it works.

    Waits base_s, then base_s * factor, and so on, never more than max_delay_s,
    with +/- `jitter` randomness so several processes don't retry in lockstep.
    Gives up and re-raises the last error once the next wait would pass
    deadline_s (None = retry forever). Errors in `fatal` are never retried.
    on_retry(err) is called before each wait, e.g. so a UI can show the problem.
    """
    start, delay, attempt = clock(), base_s, 0
    while True:
        attempt += 1
        try:
            return fn()
        except fatal:
            raise
        except retry_on as err:
            wait = delay * random.uniform(1 - jitter, 1 + jitter)
            if deadline_s is not None and clock() - start + wait > deadline_s:
                raise
            print(
                f"[retry] {what} not ready ({type(err).__name__}), attempt {attempt}, retrying in {wait:.1f}s",
                file=sys.stderr,
                flush=True,
            )
            if on_retry:
                on_retry(err)
            sleep(wait)
            delay = min(delay * factor, max_delay_s)


def connect_kafka(client_cls, *args, deadline_s=180.0, on_retry=None, **config):
    """KafkaProducer / KafkaConsumer, retried until the broker is up."""
    return retry_with_backoff(
        lambda: client_cls(*args, **config),
        f"Kafka ({client_cls.__name__})",
        retry_on=KAFKA_RETRY_ON,
        fatal=(KafkaConfigurationError,),  # a typo in config will never fix itself
        deadline_s=deadline_s,
        on_retry=on_retry,
    )
