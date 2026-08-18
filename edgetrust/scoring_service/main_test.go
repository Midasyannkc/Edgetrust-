package main

import (
	"math"
	"testing"
)

func testModel() ModelWeights {
	return ModelWeights{
		Features:      []string{"txn_count_3min", "max_jump_speed_mph", "amount"},
		Means:         []float64{1.0, 5.0, 150.0},
		Stds:          []float64{0.4, 29.0, 85.0},
		Weights:       []float64{1.1, 0.7, -4.5},
		Bias:          -7.0,
		FlagThreshold: 0.5,
	}
}

func TestSigmoidBounds(t *testing.T) {
	if sigmoid(0) != 0.5 {
		t.Errorf("sigmoid(0) = %v, want 0.5", sigmoid(0))
	}
	if sigmoid(100) < 0.999 {
		t.Errorf("sigmoid(100) should approach 1, got %v", sigmoid(100))
	}
	if sigmoid(-100) > 0.001 {
		t.Errorf("sigmoid(-100) should approach 0, got %v", sigmoid(-100))
	}
}

func TestScoreDeterministic(t *testing.T) {
	model = testModel()
	feats := map[string]float64{"txn_count_3min": 1, "max_jump_speed_mph": 1855.0, "amount": 819.73}
	s1, _ := score(feats)
	s2, _ := score(feats)
	if s1 != s2 {
		t.Errorf("scoring the same input twice gave different results: %v vs %v", s1, s2)
	}
}

func TestScoreInRange(t *testing.T) {
	model = testModel()
	cases := []map[string]float64{
		{"txn_count_3min": 1, "max_jump_speed_mph": 0, "amount": 20},
		{"txn_count_3min": 15, "max_jump_speed_mph": 2000, "amount": 900},
		{"txn_count_3min": 0, "max_jump_speed_mph": 0, "amount": 0},
	}
	for _, c := range cases {
		s, _ := score(c)
		if math.IsNaN(s) || s < 0 || s > 1 {
			t.Errorf("score out of [0,1] range for input %v: got %v", c, s)
		}
	}
}

func TestHighVelocityAndGeoJumpScoresHigherThanNormal(t *testing.T) {
	model = testModel()
	normal := map[string]float64{"txn_count_3min": 1, "max_jump_speed_mph": 0, "amount": 50}
	suspicious := map[string]float64{"txn_count_3min": 12, "max_jump_speed_mph": 1800, "amount": 50}

	normalScore, _ := score(normal)
	suspiciousScore, _ := score(suspicious)

	if suspiciousScore <= normalScore {
		t.Errorf("expected suspicious pattern to score higher: normal=%v suspicious=%v", normalScore, suspiciousScore)
	}
}

func TestFlagThresholdRespected(t *testing.T) {
	model = testModel()
	// deliberately extreme velocity + geo-jump should cross the 0.5 threshold
	feats := map[string]float64{"txn_count_3min": 15, "max_jump_speed_mph": 2000, "amount": 900}
	s, _ := score(feats)
	if s < model.FlagThreshold {
		t.Errorf("expected an extreme velocity+geo-jump pattern to be flagged, got score %v below threshold %v", s, model.FlagThreshold)
	}
}

func TestLoadModelRejectsMissingFile(t *testing.T) {
	_, err := loadModel("does_not_exist.json")
	if err == nil {
		t.Error("expected an error loading a nonexistent model file, got nil")
	}
}
