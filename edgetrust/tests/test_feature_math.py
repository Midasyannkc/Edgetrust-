"""
Unit tests for the pure-math functions in the Flink feature job: haversine
distance and the derived geo-jump speed calculation. These run without
starting a Flink environment, so they execute fast and independent of the
JVM/py4j bridge, keeping the fast feedback loop the streaming job itself
can't offer.
"""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stream_processing"))
from flink_feature_job import haversine_miles


def test_haversine_zero_distance():
    assert haversine_miles(40.0, -74.0, 40.0, -74.0) == 0.0


def test_haversine_known_distance_dallas_to_nyc():
    # Dallas to NYC is roughly 1370-1380 miles great-circle
    dist = haversine_miles(32.7767, -96.7970, 40.7128, -74.0060)
    assert 1350 < dist < 1400, f"expected ~1370 miles, got {dist}"


def test_haversine_symmetric():
    d1 = haversine_miles(32.7767, -96.7970, 40.7128, -74.0060)
    d2 = haversine_miles(40.7128, -74.0060, 32.7767, -96.7970)
    assert math.isclose(d1, d2, rel_tol=1e-9)


def test_geo_jump_speed_flags_impossible_travel():
    # same account, 2 minutes apart, Dallas to NYC: physically impossible
    dist = haversine_miles(32.7767, -96.7970, 40.7128, -74.0060)
    dt_hours = 2 / 60
    implied_speed = dist / dt_hours
    assert implied_speed > 500, "a 2-minute cross-country jump should imply an impossible speed"


def test_geo_jump_speed_normal_local_travel():
    # same account, 10 minutes apart, both within a few miles: normal
    dist = haversine_miles(32.7767, -96.7970, 32.7800, -96.8000)
    dt_hours = 10 / 60
    implied_speed = dist / dt_hours
    assert implied_speed < 60, "local travel within a metro area should imply a plausible driving speed"


if __name__ == "__main__":
    test_haversine_zero_distance()
    test_haversine_known_distance_dallas_to_nyc()
    test_haversine_symmetric()
    test_geo_jump_speed_flags_impossible_travel()
    test_geo_jump_speed_normal_local_travel()
    print("All feature math tests passed")
