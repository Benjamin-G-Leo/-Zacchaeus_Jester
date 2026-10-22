from fraud.rules import UserState


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
