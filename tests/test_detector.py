import json

import pytest

from fraud.detector import handle
from fraud.state import MemoryStore


def raw(**changes):
    base = {"tx_id": "t1", "user_id": "u1", "amount": 10.0, "city": "Mumbai", "ts": 1000.0}
    return json.dumps({**base, **changes}).encode()


def test_good_message_is_processed():
    alerts, reason = handle(raw(), MemoryStore())
    assert alerts == [] and reason is None


@pytest.mark.parametrize("message,start", [(b"{not json", "not JSON"), (b"\xff\xfe", "not JSON"), (b"[]", "not an object")])
def test_unreadable_messages_are_rejected_not_raised(message, start):
    alerts, reason = handle(message, MemoryStore())
    assert alerts == [] and reason.startswith(start)


def test_unknown_city_is_rejected_and_never_reaches_state():
    store = MemoryStore()
    alerts, reason = handle(raw(city="Atlantis"), store)
    assert "unknown city" in reason
    assert store.users == {}  # the bad city is not remembered, so it cannot crash later swipes


def test_nan_amount_is_rejected():
    assert handle(raw(amount=float("nan")), MemoryStore())[1] == "bad amount"


def test_a_bad_message_does_not_stop_the_next_good_ones():
    store = MemoryStore()
    handle(raw(tx_id="a", city="Mumbai", ts=1000.0), store)
    assert handle(raw(tx_id="b", city="Atlantis", ts=1100.0), store)[1] is not None
    alerts, reason = handle(raw(tx_id="c", city="New York", ts=1300.0), store)
    assert reason is None and [a["rule"] for a in alerts] == ["impossible_travel"]


def test_infrastructure_errors_are_not_swallowed():
    """Redis being down must stop the detector (and replay), not silently drop messages."""

    class Broken:
        def process(self, tx):
            raise ConnectionError("redis is down")

    with pytest.raises(ConnectionError):
        handle(raw(), Broken())
