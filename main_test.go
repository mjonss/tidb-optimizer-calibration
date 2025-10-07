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
	// Test configuration: 1M rows with 10% selectivity
	rowCounts := []int{1000000}
	selectivities := []float64{1.0, 2.0, 5.0, 10.0, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1}

	// Run the optimizer tests
	setupLogging("debug")
	err := RunOptimizerTests(rowCounts, selectivities, 3)
	if err != nil {
		t.Fatalf("RunOptimizerTests failed: %v", err)
	}
}
