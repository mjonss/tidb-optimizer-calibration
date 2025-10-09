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
	err := CheckAndSetupTables(rowCounts, selectivities, 500)
	if err != nil {
		t.Fatalf("CheckAndSetupTables failed: %v", err)
	}
	results := RunOptimizerTests(rowCounts, selectivities, 1)
	outputDetailedResultsTable(results)
	outputAggregatedResultsTable(results)
}

func TestMulti(t *testing.T) {
	rowCounts := []int{1000, 10000, 100000}
	selectivities := []float64{0.02, 0.05, 0.075, 0.1, 0.15, 0.2}

	// Run the optimizer tests
	setupLogging("debug")
	err := CheckAndSetupTables(rowCounts, selectivities, 500)
	if err != nil {
		t.Fatalf("CheckAndSetupTables failed: %v", err)
	}
	results := RunOptimizerTests(rowCounts, selectivities, 3)
	outputDetailedResultsTable(results)
	outputAggregatedResultsTable(results)
}
