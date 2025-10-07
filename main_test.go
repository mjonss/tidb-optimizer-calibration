package main

import (
	"testing"
)

func TestSimple(t *testing.T) {
	// Test configuration: 1M rows with 10% selectivity
	rowCounts := []int{1000000}
	selectivities := []float64{0.1}

	// Run the optimizer tests
	setupLogging("debug")
	err := RunOptimizerTests(rowCounts, selectivities, 1)
	if err != nil {
		t.Fatalf("RunOptimizerTests failed: %v", err)
	}
}

func TestMulti(t *testing.T) {
	rowCounts := []int{1000000, 10000000}
	selectivities := []float64{0.02, 0.05, 0.075, 0.1, 0.15, 0.2}

	// Run the optimizer tests
	setupLogging("info")
	err := RunOptimizerTests(rowCounts, selectivities, 7)
	if err != nil {
		t.Fatalf("RunOptimizerTests failed: %v", err)
	}
}
