"""
Trains a logistic regression fraud classifier on the features Flink
computed (txn_count_3min, max_jump_speed_mph, amount), using the injected
ground-truth labels from the synthetic generator.

Logistic regression is a deliberate choice, not a limitation: at the
scoring-latency budget this system targets (sub-200ms, no GPU in the hot
path), a linear model over well-engineered features is what most real-time
fraud systems actually run in production, with a gradient-boosted model
reserved for the offline/batch re-scoring path. The interesting engineering
is in the streaming feature pipeline, not the model complexity.

Exports coefficients to scoring_service/model_weights.json, a simple format
the Go service loads directly, no ONNX runtime or cross-language model
server required for a model this small, another realistic production
tradeoff: don't reach for a heavyweight serving stack when a JSON file and
five multiplications will hit the same latency and accuracy target.
"""

import json
import os
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler

BASE = os.path.dirname(os.path.abspath(__file__))
FEATURES_PATH = os.path.join(os.path.dirname(BASE), "stream_processing", "features.jsonl")
OUTPUT_PATH = os.path.join(os.path.dirname(BASE), "scoring_service", "model_weights.json")

rows = []
with open(FEATURES_PATH) as f:
    for line in f:
        e = json.loads(line)
        rows.append({
            "txn_count_3min": e["txn_count_3min"],
            "max_jump_speed_mph": e["max_jump_speed_mph"],
            "amount": e["amount"],
            "label": int(e["_is_fraud_injected"]),
        })

df = pd.DataFrame(rows)
print(f"Training rows: {len(df)}, fraud-labeled: {df['label'].sum()}")

FEATURES = ["txn_count_3min", "max_jump_speed_mph", "amount"]
X = df[FEATURES]
y = df["label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=7, stratify=y
)

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)

model = LogisticRegression(class_weight="balanced", random_state=7)
model.fit(X_train_s, y_train)

y_pred = model.predict(X_test_s)
y_proba = model.predict_proba(X_test_s)[:, 1]

print(classification_report(y_test, y_pred, digits=3))
print(f"ROC-AUC: {roc_auc_score(y_test, y_proba):.3f}")

# Fold the StandardScaler into the exported weights so the Go service can
# score a raw feature vector directly: score = sigmoid(w . ((x - mean) / std) + b)
export = {
    "features": FEATURES,
    "means": scaler.mean_.tolist(),
    "stds": scaler.scale_.tolist(),
    "weights": model.coef_[0].tolist(),
    "bias": float(model.intercept_[0]),
    "flag_threshold": 0.5,
}

with open(OUTPUT_PATH, "w") as f:
    json.dump(export, f, indent=2)

print(f"\nExported model weights to {OUTPUT_PATH}")
