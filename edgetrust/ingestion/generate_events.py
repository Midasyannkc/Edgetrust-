"""
Generates a synthetic stream of transaction events matching the
TransactionEvent contract in proto/fraud_scorer.proto.

Two fraud patterns are deliberately embedded, chosen because they are the
two canonical real-time fraud signals every payments company screens for:

  1. VELOCITY: a burst of transactions from the same account in a short
     window, the classic "card testing" or account-takeover pattern.
  2. GEO-JUMP: two transactions from the same account, physically too far
     apart to be the same person, in too short a time window (a "traveled
     500 miles in 2 minutes" pattern).

Legitimate transactions dominate the stream (~95%), fraud patterns are
injected at a known rate so the feature engineering and scoring stages can
be validated against ground truth.

Output: JSONL, one event per line, written to ingestion/events.jsonl,
standing in for what a Kafka/Redpanda topic would carry in production.
See stream_processing/README.md for why local dev reads this file directly
instead of a live broker.
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

random.seed(7)

OUT_PATH = os.path.join(os.path.dirname(__file__), "events.jsonl")

N_ACCOUNTS = 300
N_EVENTS = 15000
FRAUD_RATE = 0.03  # 3% of accounts get a fraud pattern injected

# rough US city centers, used to place legitimate transactions near a
# consistent "home" location per account, and fraud geo-jumps far from it
CITIES = [
    (32.7767, -96.7970),  # Dallas
    (40.7128, -74.0060),  # NYC
    (34.0522, -118.2437), # LA
    (41.8781, -87.6298),  # Chicago
    (29.7604, -95.3698),  # Houston
]

MERCHANT_CATS = ["grocery", "gas", "restaurant", "electronics", "travel", "online_retail"]


def jitter(lat, lon, miles=5):
    deg = miles / 69.0
    return lat + random.uniform(-deg, deg), lon + random.uniform(-deg, deg)


def gen_account_home():
    return random.choice(CITIES)


accounts = {f"ACCT-{i:05d}": gen_account_home() for i in range(1, N_ACCOUNTS + 1)}
fraud_accounts = set(random.sample(list(accounts.keys()), int(N_ACCOUNTS * FRAUD_RATE)))

start_time = datetime(2026, 8, 1, tzinfo=timezone.utc)
events = []

for acct_id, home in accounts.items():
    n_txns = random.randint(20, 80)
    t = start_time + timedelta(minutes=random.randint(0, 500))
    for i in range(n_txns):
        lat, lon = jitter(*home)
        t = t + timedelta(minutes=random.randint(15, 240))
        events.append({
            "event_id": str(uuid.uuid4()),
            "account_id": acct_id,
            "device_id": f"DEV-{acct_id[-5:]}",
            "amount": round(random.uniform(5, 300), 2),
            "currency": "USD",
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "event_timestamp_ms": int(t.timestamp() * 1000),
            "merchant_category": random.choice(MERCHANT_CATS),
            "_is_fraud_injected": False,
        })

    if acct_id in fraud_accounts:
        pattern = random.choice(["velocity", "geo_jump"])
        base_t = t + timedelta(minutes=random.randint(30, 120))

        if pattern == "velocity":
            # burst of 8-15 transactions within 3 minutes
            burst_t = base_t
            for _ in range(random.randint(8, 15)):
                burst_t += timedelta(seconds=random.randint(5, 20))
                lat, lon = jitter(*home, miles=1)
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "account_id": acct_id,
                    "device_id": f"DEV-{acct_id[-5:]}-ALT",
                    "amount": round(random.uniform(1, 50), 2),
                    "currency": "USD",
                    "lat": round(lat, 5),
                    "lon": round(lon, 5),
                    "event_timestamp_ms": int(burst_t.timestamp() * 1000),
                    "merchant_category": "online_retail",
                    "_is_fraud_injected": True,
                })

        else:  # geo_jump
            far_lat, far_lon = random.choice([c for c in CITIES if c != home])
            jump_t = base_t + timedelta(minutes=2)
            events.append({
                "event_id": str(uuid.uuid4()),
                "account_id": acct_id,
                "device_id": f"DEV-{acct_id[-5:]}-ALT",
                "amount": round(random.uniform(50, 900), 2),
                "currency": "USD",
                "lat": far_lat,
                "lon": far_lon,
                "event_timestamp_ms": int(jump_t.timestamp() * 1000),
                "merchant_category": "electronics",
                "_is_fraud_injected": True,
            })

events.sort(key=lambda e: e["event_timestamp_ms"])

with open(OUT_PATH, "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

n_fraud = sum(1 for e in events if e["_is_fraud_injected"])
print(f"Wrote {len(events)} events to {OUT_PATH}")
print(f"  legitimate: {len(events) - n_fraud}")
print(f"  fraud-pattern injected: {n_fraud} across {len(fraud_accounts)} accounts")
