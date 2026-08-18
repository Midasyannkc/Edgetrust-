# EdgeTrust: Real-Time Fraud Scoring at the Edge

## Problem

Batch fraud detection catches fraud after the damage is done. A card-testing
burst or an account takeover needs a decision in milliseconds, not
tomorrow's overnight job. This project builds the actual real-time path:
event ingestion, stateful streaming feature computation, an online feature
store, and a low-latency scoring service, end to end, tested, and running.

## Architecture

```
events (ingestion/)
    -> PyFlink stateful streaming job (stream_processing/)
    -> Redis online feature store (feature_store/)
    -> Go scoring service, sub-millisecond compute (scoring_service/)
```

Production target: Redpanda (event backbone) -> Flink on Kubernetes ->
ElastiCache Redis -> Go scoring service behind a gRPC interface, provisioned
by Terraform (`infra/`). See "What's real vs. what's production-target"
below for exactly which pieces run in this repo today.

## What's actually being detected

Two fraud patterns, generated with ground-truth labels so detection can be
validated, not just asserted:

1. **Velocity**: a burst of 8-15 transactions from one account within
   minutes, the classic card-testing pattern.
2. **Geo-jump**: two transactions from the same account, far enough apart
   and close enough in time to imply a physically impossible travel speed.

`stream_processing/flink_feature_job.py` computes both as true Flink
`KeyedProcessFunction` state, incrementally, per account, across an
unbounded stream, not a batch recompute.

## What's real vs. what's production-target

This repo follows the same honesty standard as the Bank Lakehouse Migration
project: pieces that can genuinely run in a sandboxed build environment are
built and tested; pieces that require infrastructure this environment
cannot reach (a live Redpanda broker, a Kubernetes cluster, protoc's module
graph) are written to the real production interface but documented as such,
not silently faked.

| Component | Status |
|---|---|
| PyFlink stateful streaming job | **Real.** Runs locally against a JVM, computes true keyed state, tested against 15,158 synthetic events. |
| Redis feature store | **Real.** Local Redis instance, real HSET/HGETALL traffic. |
| Go scoring service | **Real.** Compiles, runs, serves live HTTP traffic, p99 6.5ms including full round trip. |
| Fraud model | **Real.** Logistic regression trained on Flink-computed features, ROC-AUC 0.998, exported and loaded by the Go service. |
| gRPC interface | **Production-target.** Defined in `proto/fraud_scorer.proto`. The Go service implements identical logic over HTTP/JSON locally because generating the grpc-go stubs requires reaching proxy.golang.org, unavailable in this build sandbox. Swapping transport is the only change a real deployment needs. |
| Redpanda ingestion | **Production-target.** The Flink job reads a JSONL file standing in for a live topic; swapping the source connector is the only change required. |
| Terraform / Kubernetes | **Production-target.** Written, not applied (no cloud credentials in this environment), same pattern as the Databricks job spec in the Bank Lakehouse Migration project. |

## Why the model is simple on purpose

The exported model is logistic regression over three engineered features,
not a deep model. At a sub-200ms latency budget with no GPU in the hot
path, a linear model over well-engineered streaming features is what most
real-time fraud systems actually run in production. The engineering weight
in this project is in the streaming feature pipeline, not model complexity,
which is also where the actual latency and correctness risk lives.

The model is tuned for high recall given severe class imbalance (58 fraud
events out of 15,158): it catches all labeled fraud in the test set at the
cost of some false positives, a deliberate tradeoff, not an accident, given
that a missed fraud event is far more expensive than a false positive
review.

## Running it locally

```bash
# 1. Generate synthetic transaction events with embedded fraud patterns
cd ingestion && python3 generate_events.py

# 2. Run the real PyFlink streaming job (isolated venv, see below)
cd ../ && python3 -m venv .flink_venv && . .flink_venv/bin/activate
pip install apache-flink
cd stream_processing && python3 flink_feature_job.py

# 3. Start Redis and load computed features
redis-server --daemonize yes --port 6390
cd ../feature_store && python3 load_features.py

# 4. Train and export the fraud model
cd ../model_training && python3 train_model.py

# 5. Build and run the Go scoring service
cd ../scoring_service && go build -o edgetrust_scorer . && ./edgetrust_scorer

# 6. Score a transaction
curl -X POST localhost:8080/score -d '{"event_id":"t1","account_id":"ACCT-00053","amount":819.73,"lat":40.71,"lon":-74.0,"event_timestamp_ms":1785731400000}'
```

## Test suite

```bash
cd scoring_service && go test -v ./...        # 6 tests, scoring logic
cd tests && python3 test_feature_math.py       # 5 tests, geo-jump math
```

11 of 11 tests pass. The Go tests validate scoring logic independent of
Redis or HTTP; the Python tests validate the haversine distance math
independent of the Flink runtime, so both suites run in milliseconds
without standing up infrastructure.

## Why this maps to the target roles

This is deliberately built to close a specific, named gap: nothing else in
the surrounding portfolio touches sub-second streaming infrastructure, a
compiled systems language, or an online feature store. DoorDash, Uber, and
similar marketplace companies run fraud and trust decisions in exactly this
shape, event stream in, stateful feature computation, low-latency scoring
service out, and screen for direct experience with this pattern, not just
batch pipeline experience.
