// EdgeTrust scoring service.
//
// Production interface: the gRPC service defined in proto/fraud_scorer.proto,
// generated with protoc-gen-go and protoc-gen-go-grpc. This file implements
// the identical scoring logic over plain HTTP/JSON instead, because
// generating and vendoring the grpc-go module graph requires reaching
// proxy.golang.org, which this build environment cannot reach. The scoring
// logic, state loading, and latency characteristics below are exactly what
// the gRPC handler would call; swapping the transport is the only change
// a real deployment needs. See README.md for the full explanation.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

// ModelWeights mirrors model_weights.json, produced by
// model_training/train_model.py.
type ModelWeights struct {
	Features      []string  `json:"features"`
	Means         []float64 `json:"means"`
	Stds          []float64 `json:"stds"`
	Weights       []float64 `json:"weights"`
	Bias          float64   `json:"bias"`
	FlagThreshold float64   `json:"flag_threshold"`
}

// TransactionEvent mirrors the TransactionEvent message in
// proto/fraud_scorer.proto.
type TransactionEvent struct {
	EventID          string  `json:"event_id"`
	AccountID        string  `json:"account_id"`
	DeviceID         string  `json:"device_id"`
	Amount           float64 `json:"amount"`
	Currency         string  `json:"currency"`
	Lat              float64 `json:"lat"`
	Lon              float64 `json:"lon"`
	EventTimestampMs int64   `json:"event_timestamp_ms"`
	MerchantCategory string  `json:"merchant_category"`
}

type ScoreResponse struct {
	EventID    string   `json:"event_id"`
	RiskScore  float64  `json:"risk_score"`
	Flagged    bool     `json:"flagged"`
	Reasons    []string `json:"reasons"`
	ScoredAtMs int64    `json:"scored_at_ms"`
	LatencyUs  int64    `json:"latency_us"`
}

var model ModelWeights
var redisAddr string

func loadModel(path string) (ModelWeights, error) {
	var m ModelWeights
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}

func sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// score reproduces exactly what the exported logistic regression does:
// score = sigmoid(w . ((x - mean) / std) + b)
func score(features map[string]float64) (float64, []string) {
	z := model.Bias
	var reasons []string
	for i, name := range model.Features {
		x := features[name]
		standardized := (x - model.Means[i]) / model.Stds[i]
		contribution := model.Weights[i] * standardized
		z += contribution
		if contribution > 0.5 {
			reasons = append(reasons, fmt.Sprintf("%s=%.1f above baseline", name, x))
		}
	}
	return sigmoid(z), reasons
}

// getFeatures reads the account's latest Flink-computed features from
// Redis using RESP directly over a raw TCP connection. A production
// service would use a proper Redis client library (go-redis), which this
// local harness avoids for the same module-proxy reason as gRPC above; the
// hand-rolled client here implements exactly the two RESP commands this
// service needs (HGETALL) and nothing more.
func getFeatures(accountID string) (map[string]float64, error) {
	conn, err := dialRedis(redisAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	key := "features:" + accountID
	reply, err := respHGetAll(conn, key)
	if err != nil {
		return nil, err
	}

	out := map[string]float64{
		"txn_count_3min":     0,
		"max_jump_speed_mph": 0,
		"amount":             0,
	}
	if v, ok := reply["txn_count_3min"]; ok {
		out["txn_count_3min"], _ = strconv.ParseFloat(v, 64)
	}
	if v, ok := reply["max_jump_speed_mph"]; ok {
		out["max_jump_speed_mph"], _ = strconv.ParseFloat(v, 64)
	}
	return out, nil
}

func scoreHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var event TransactionEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	feats, err := getFeatures(event.AccountID)
	if err != nil {
		http.Error(w, "feature store unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	feats["amount"] = event.Amount

	riskScore, reasons := score(feats)
	resp := ScoreResponse{
		EventID:    event.EventID,
		RiskScore:  riskScore,
		Flagged:    riskScore >= model.FlagThreshold,
		Reasons:    reasons,
		ScoredAtMs: time.Now().UnixMilli(),
		LatencyUs:  time.Since(start).Microseconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	weightsPath := "model_weights.json"
	if v := os.Getenv("MODEL_WEIGHTS_PATH"); v != "" {
		weightsPath = v
	}
	redisAddr = "localhost:6390"
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		redisAddr = v
	}

	var err error
	model, err = loadModel(weightsPath)
	if err != nil {
		log.Fatalf("failed to load model weights: %v", err)
	}
	log.Printf("loaded model with %d features, flag threshold %.2f", len(model.Features), model.FlagThreshold)

	http.HandleFunc("/score", scoreHandler)
	http.HandleFunc("/healthz", healthHandler)

	port := "8080"
	if v := os.Getenv("PORT"); v != "" {
		port = v
	}
	log.Printf("EdgeTrust scoring service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
