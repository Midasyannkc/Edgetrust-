"""
Loads the latest computed features per account into Redis, keyed for O(1)
lookup at scoring time. This is the online feature store: the scoring
service never recomputes a feature, it only reads the most recent value
Flink already wrote here.

Key schema: features:{account_id} -> hash of latest feature values
TTL: 24 hours, so a dormant account's stale features age out rather than
being served indefinitely.
"""

import json
import os
import redis

BASE = os.path.dirname(os.path.abspath(__file__))
FEATURES_PATH = os.path.join(os.path.dirname(BASE), "stream_processing", "features.jsonl")

FEATURE_TTL_SECONDS = 24 * 60 * 60

r = redis.Redis(host="localhost", port=6390, decode_responses=True)

count = 0
with open(FEATURES_PATH) as f:
    for line in f:
        event = json.loads(line)
        key = f"features:{event['account_id']}"
        r.hset(key, mapping={
            "last_event_id": event["event_id"],
            "txn_count_3min": event["txn_count_3min"],
            "max_jump_speed_mph": event["max_jump_speed_mph"],
            "last_amount": event["amount"],
            "last_lat": event["lat"],
            "last_lon": event["lon"],
            "last_event_timestamp_ms": event["event_timestamp_ms"],
        })
        r.expire(key, FEATURE_TTL_SECONDS)
        count += 1

print(f"Loaded features for {count} events into Redis (final state per account retained)")
print(f"Distinct accounts in feature store: {len(r.keys('features:*'))}")
