import threading

from fraud.collector import Collector


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
