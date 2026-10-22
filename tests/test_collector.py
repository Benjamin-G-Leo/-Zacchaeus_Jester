import json
import threading

import pytest

from fraud.collector import Collector, parse_alert


def alert(i, rule="velocity"):
    return {"rule": rule, "tx_id": f"t{i}", "user_id": "u1", "n": i}


def make(**kw):
    return Collector(start=False, **kw)  # no Kafka threads


def test_page_gets_alerts_newest_first():
    c = make()
    for i in range(3):
        c.put_alert(alert(i))
    snap = c.snapshot()
    assert [a["n"] for a in snap["alerts"]] == [2, 1, 0]
    assert snap["alert_total"] == 3


def test_draining_twice_does_not_duplicate():
    c = make()
    c.put_alert(alert(1))
    c.snapshot()
    assert len(c.snapshot()["alerts"]) == 1


def test_full_queue_drops_oldest_but_counters_stay_exact():
    c = make(inbox_max=3)  # nobody drains while we push, like a server with no page open
    for i in range(10):
        c.put_alert(alert(i, rule="impossible_travel" if i % 2 else "velocity"))
    snap = c.snapshot()
    assert [a["n"] for a in snap["alerts"]] == [9, 8, 7]  # only the newest survive
    assert snap["alert_total"] == 10
    assert snap["by_rule"] == {"velocity": 5, "impossible_travel": 5}


def test_recent_list_is_bounded():
    c = make(recent_max=5)
    for i in range(20):
        c.put_alert(alert(i))
    assert len(c.snapshot()["alerts"]) == 5


def test_tx_rate_uses_the_ten_second_window():
    c = make()
    for _ in range(50):
        c.count_tx()
    snap = c.snapshot()
    assert snap["tx_total"] == 50
    assert snap["tx_per_sec"] == 5.0


def test_tx_history_has_one_slot_per_second_oldest_first():
    c = make()
    for _ in range(7):
        c.count_tx()
    hist = c.snapshot()["tx_history"]
    assert len(hist) == 60
    assert sum(hist) == 7 and hist[-1] + hist[-2] == 7  # all in the newest second or two
    assert hist[0] == 0


def test_old_tx_buckets_are_dropped():
    c = make()
    c.count_tx()
    c.tx_buckets[0][0] -= 3600  # an hour old
    c.count_tx()
    assert len(c.tx_buckets) == 1  # the stale one is gone, memory stays bounded


def test_active_alerts_only_counts_the_last_minute():
    c = make()
    for i in range(3):
        c.put_alert(alert(i))
    c.alert_times[0] -= 120  # one of them is two minutes old
    snap = c.snapshot()
    assert snap["active_alerts"] == 2
    assert snap["alert_total"] == 3  # the lifetime total is unaffected


def test_readers_and_page_can_run_at_the_same_time():
    c = make(inbox_max=100)
    stop = threading.Event()
    errors = []

    def page():
        try:
            while not stop.is_set():
                c.snapshot()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    drainer = threading.Thread(target=page)
    drainer.start()

    def reader(base):
        for i in range(500):
            c.put_alert(alert(base + i))

    readers = [threading.Thread(target=reader, args=(k * 1000,)) for k in range(4)]
    for t in readers:
        t.start()
    for t in readers:
        t.join()
    stop.set()
    drainer.join()
    assert errors == []
    assert c.snapshot()["alert_total"] == 2000


GOOD_ALERT = {"rule": "velocity", "user_id": "u1", "tx_id": "t1", "city": "London", "detail": "x", "amount": 5.0, "ts": 1.0}


def raw(**changes):
    return json.dumps({**GOOD_ALERT, **changes}).encode()


def test_good_alert_parses():
    assert parse_alert(raw())["user_id"] == "u1"


@pytest.mark.parametrize(
    "message",
    [b"not json", b"[]", b"\xff\xfe", raw(rule="nope"), raw(rule=None), raw(user_id=5), raw(amount="1"),
     raw(amount=float("nan")), raw(ts=None), raw(city=None)],
)
def test_bad_alerts_are_rejected_not_raised(message):
    assert parse_alert(message) is None


@pytest.mark.parametrize("missing", ["rule", "user_id", "tx_id", "city", "detail", "amount", "ts"])
def test_alert_missing_a_field_is_rejected(missing):
    d = dict(GOOD_ALERT)
    del d[missing]
    assert parse_alert(json.dumps(d).encode()) is None  # one of these froze the page with a KeyError


def test_wrongly_typed_extras_are_dropped_not_fatal():
    a = parse_alert(raw(distance_km="far", from_city=5, count=6))
    assert "distance_km" not in a and "from_city" not in a
    assert a["count"] == 6
