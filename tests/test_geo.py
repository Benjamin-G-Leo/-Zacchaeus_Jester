import random

import pytest

from fraud.common import CITIES, haversine_km


@pytest.mark.parametrize(
    "a,b,low,high",
    [
        ("Mumbai", "Delhi", 1130, 1170),  # about 1150 km
        ("London", "Paris", 335, 350),  # about 344 km
        ("New York", "London", 5540, 5600),  # about 5570 km
        ("Delhi", "Sydney", 10300, 10500),
    ],
)
def test_matches_known_distances(a, b, low, high):
    assert low <= haversine_km(CITIES[a], CITIES[b]) <= high


def test_same_point_and_symmetry():
    assert haversine_km(CITIES["Tokyo"], CITIES["Tokyo"]) == 0
    assert haversine_km(CITIES["Tokyo"], CITIES["Dubai"]) == pytest.approx(haversine_km(CITIES["Dubai"], CITIES["Tokyo"]))


def test_crossing_the_date_line_takes_the_short_way():
    # 179E to 179W is 2 degrees of longitude (about 222 km), not 358
    assert haversine_km((0, 179), (0, -179)) == pytest.approx(222, abs=2)


def test_antipodal_points_do_not_blow_up():
    rng = random.Random(3)
    for _ in range(5000):
        lat, lon = rng.uniform(-90, 90), rng.uniform(-180, 180)
        anti = (-lat, lon - 180 if lon > 0 else lon + 180)
        assert haversine_km((lat, lon), anti) == pytest.approx(20015, rel=0.01)
