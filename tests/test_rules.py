import pytest

from fraud.rules import UserState, validate


def tx(ts, amount=40.0, city="Mumbai", n=[0]):
    n[0] += 1
    return {"tx_id": f"t{n[0]}", "user_id": "u1", "amount": amount, "city": city, "ts": ts}


def rules(alerts):
    return [a["rule"] for a in alerts]


def test_normal_activity_is_quiet():
    s = UserState()
    out = []
    for i in range(30):
        out += s.process(tx(i * 600, 40 + (i % 5)))
    assert out == []


def test_impossible_travel_india_to_new_york_in_5_min():
    s = UserState()
    s.process(tx(1000, city="Mumbai"))
    out = s.process(tx(1300, city="New York"))
    assert rules(out) == ["impossible_travel"]
    assert "Mumbai -> New York" in out[0]["detail"]


def test_travel_alert_carries_the_numbers_behind_it():
    s = UserState()
    s.process(tx(1000, city="Mumbai"))
    a = s.process(tx(1300, city="New York"))[0]
    assert a["from_city"] == "Mumbai"
    assert a["elapsed_s"] == 300
    assert 12000 < a["distance_km"] < 13000  # Mumbai to New York, about 12,500 km
    assert a["speed_kmh"] == round(a["distance_km"] / (300 / 3600))


def test_velocity_alert_carries_count_and_window():
    s = UserState()
    out = []
    for i in range(7):
        out += s.process(tx(i))
    assert out[0]["count"] == 6 and out[0]["window_s"] == 10


def test_amount_alert_carries_typical_and_multiple():
    s = UserState()
    for i in range(10):
        s.process(tx(i * 600, 40))
    a = s.process(tx(7000, 800))[0]
    assert a["typical"] == 40 and a["multiple"] == 20 and a["samples"] == 10


def test_travel_with_enough_time_is_fine():
    s = UserState()
    s.process(tx(0, city="Mumbai"))
    assert s.process(tx(24 * 3600, city="New York")) == []


def test_reachable_hop_not_flagged():
    s = UserState()
    s.process(tx(0, city="Mumbai"))
    assert s.process(tx(3 * 3600, city="Delhi")) == []  # ~1150 km in 3h, a flight


def test_short_hop_under_min_distance_not_flagged():
    s = UserState()
    s.process(tx(0, city="London"))
    assert s.process(tx(60, city="Paris")) == []  # ~340 km, below the 500 km floor


def test_velocity_burst_alerts_once_per_cooldown():
    s = UserState()
    out = []
    for i in range(9):
        out += s.process(tx(i))  # 9 swipes in 9 seconds
    assert rules(out) == ["velocity"]


def test_velocity_window_slides():
    s = UserState()
    out = []
    for i in range(20):
        out += s.process(tx(i * 3))  # one every 3s -> only 4 in any 10s window
    assert out == []


def test_amount_spike():
    s = UserState()
    for i in range(10):
        s.process(tx(i * 600, 40 + (i % 3)))
    out = s.process(tx(7000, 900))
    assert rules(out) == ["amount_spike"]


def test_spike_does_not_poison_baseline():
    s = UserState()
    for i in range(10):
        s.process(tx(i * 600, 40))
    s.process(tx(7000, 900))
    assert s.mean < 50


def test_zero_average_history_does_not_crash():
    s = UserState()
    for i in range(6):
        s.process(tx(i * 100, amount=0.0))  # six $0 card checks
    # "times typical" means nothing when typical is $0. The old code divided by zero here.
    assert s.process(tx(700, amount=25.0)) == []


def test_second_spike_inside_cooldown_does_not_poison_the_baseline():
    s = UserState()
    for i in range(10):
        s.process(tx(i * 600, 40.0))
    assert rules(s.process(tx(7000, 800.0))) == ["amount_spike"]
    assert s.process(tx(7005, 800.0)) == []  # held back by the 30s cooldown...
    assert s.mean == pytest.approx(40, abs=1)  # ...but kept out of "normal" (it used to jump to ~109)
    assert rules(s.process(tx(9000, 800.0))) == ["amount_spike"]  # so a later spike is still caught


def test_two_far_cities_at_the_same_instant_is_flagged():
    s = UserState()
    s.process(tx(1000, city="Mumbai"))
    assert rules(s.process(tx(1000, city="New York"))) == ["impossible_travel"]


def test_old_swipes_behind_a_future_dated_one_are_dropped():
    s = UserState()
    out = []
    out += s.process(tx(0))
    out += s.process(tx(120))  # future-dated, parks at the head of the window
    for i in range(1, 12):
        out += s.process(tx(i * 10))  # one swipe every 10s never makes 6 in 10s
    assert out == []  # the in-memory window used to keep stale entries behind it and raise 3 false alerts


def test_future_dated_swipe_is_not_counted_in_the_window():
    s = UserState()
    s.process(tx(1000))
    out = []
    for i in range(5):
        out += s.process(tx(i))  # five real swipes in 5s, plus one future-dated entry
    assert out == []


def test_a_stored_city_we_do_not_know_heals_instead_of_crashing():
    """Bad state saved before validation existed must not crash the detector on every restart."""
    s = UserState.from_fields({"last_ts": "1000", "last_city": "Atlantis", "n": "0", "mean": "0", "m2": "0"})
    assert s.process(tx(1100, city="Mumbai")) == []  # no previous location to compare against
    assert s.last_city == "Mumbai"  # and the bad value is overwritten
    assert rules(s.process(tx(1400, city="New York"))) == ["impossible_travel"]  # detection works again


GOOD = {"tx_id": "t", "user_id": "u", "amount": 10.0, "city": "Mumbai", "ts": 1.0}


def test_validate_accepts_a_good_transaction():
    assert validate(GOOD) is None


@pytest.mark.parametrize("bad", [None, [], "x", 5])
def test_validate_rejects_non_objects(bad):
    assert validate(bad) == "not an object"


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"city": "Atlantis"}, "unknown city"),
        ({"city": None}, "unknown city"),
        ({"city": ["Mumbai"]}, "unknown city"),
        ({"amount": float("nan")}, "bad amount"),
        ({"amount": float("inf")}, "bad amount"),
        ({"amount": "10"}, "bad amount"),
        ({"amount": True}, "bad amount"),
        ({"amount": -5}, "negative amount"),
        ({"ts": None}, "bad ts"),
        ({"user_id": ""}, "missing user_id"),
        ({"tx_id": 5}, "missing tx_id"),
    ],
)
def test_validate_rejects(change, reason):
    assert reason in validate({**GOOD, **change})
