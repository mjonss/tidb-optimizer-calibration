package main

import (
	"fmt"
	"math/rand"
)

// TestScenario represents a test scenario for optimizer validation
type TestScenario struct {
	ID          string `json:"id"`
	Variant     string `json:"variant"`
	Name        string `json:"name"`
	Query       string `json:"original_query"`
	TableName   string `json:"table_name"`
	RowCount    int    `json:"row_count"`
	ExplainOnly bool   `json:"explain_only"`
}

// TestExecutionResult represents the result of executing a test query
type TestExecutionResult struct {
	ScenarioID  string
	Variant     string
	Query       string
	PlanType    string
	Plan        *ExecutionPlan
	ExplainOnly bool
}

// GetNumRows return number of matching rows from table rows vs selectivity
func GetNumRows(rows int, sel float64) int {
	if sel < 1.0 {
		// Percentage-based selectivity (0.0 to 1.0)
		return int(float64(rows) * sel)
	}
	// Row count-based selectivity
	return int(sel)
}

// GetTestScenariosWithRowCountsAndSelectivities converts comprehensive tests to TestScenario format with custom row counts and selectivities
func GetTestScenariosWithRowCountsAndSelectivities(rowCounts []int, selectivities []float64, repetitions int) []TestScenario {
	var scenarios []TestScenario

	// Generate tests for each combination of row count and selectivity
	for _, rowCount := range rowCounts {
		tableSizeName := formatRowCountName(rowCount)

		for _, sel := range selectivities {
			// Calculate the actual value to search for based on selectivity type
			searchValue := GetNumRows(rowCount, sel)

			// Create index lookup test (without hints)
			id := fmt.Sprintf("index_%s_%s", tableSizeName, formatSelectivityName(rowCount, sel))
			indexQuery := fmt.Sprintf("SELECT * FROM t%s WHERE b = %d", tableSizeName, searchValue)

			scenario := TestScenario{
				ID:          id,
				Variant:     "ExplainOnly",
				Name:        fmt.Sprintf("Index Lookup - %s rows, %d selectivity", tableSizeName, int(sel)),
				Query:       indexQuery,
				TableName:   fmt.Sprintf("t%s", tableSizeName),
				RowCount:    rowCount,
				ExplainOnly: true,
			}
			scenarios = append(scenarios, scenario)

			query := fmt.Sprintf("SELECT /*+ FORCE_INDEX(t%s, b) */ * FROM t%s WHERE b = %d", tableSizeName, tableSizeName, searchValue)

			scenario = TestScenario{
				ID:        id,
				Variant:   "Index",
				Name:      fmt.Sprintf("Index lookup - %s rows, %d selectivity", tableSizeName, int(sel)),
				Query:     query,
				TableName: fmt.Sprintf("t%s", tableSizeName),
				RowCount:  rowCount,
			}
			for range repetitions {
				scenarios = append(scenarios, scenario)
			}

			query = fmt.Sprintf("SELECT /*+ IGNORE_INDEX(t%s, b) */ * FROM t%s WHERE b = %d", tableSizeName, tableSizeName, searchValue)

			scenario = TestScenario{
				ID:        id,
				Variant:   "TableScan",
				Name:      fmt.Sprintf("Table Scan - %s rows, %d selectivity", tableSizeName, int(sel)),
				Query:     query,
				TableName: fmt.Sprintf("t%s", tableSizeName),
				RowCount:  rowCount,
			}
			for range repetitions {
				scenarios = append(scenarios, scenario)
			}
		}
	}
	// Make sure they are run in random order.
	rand.Shuffle(len(scenarios), func(i, j int) {
		scenarios[i], scenarios[j] = scenarios[j], scenarios[i]
	})
	return scenarios
}

// formatSelectivityName formats a selectivity value into a scenario ID format
func formatSelectivityName(r int, v float64) string {
	// Convert to a safe format for scenario IDs
	return fmt.Sprintf("%d", GetNumRows(r, v))
}

// formatRowCountName formats a row count into a table name format
func formatRowCountName(count int) string {
	switch {
	case count >= 1000000:
		return fmt.Sprintf("%dM", count/1000000)
	case count >= 1000:
		return fmt.Sprintf("%dK", count/1000)
	default:
		return fmt.Sprintf("%d", count)
	}
}
