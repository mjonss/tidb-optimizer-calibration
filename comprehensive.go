package main

import (
	"fmt"
	"time"
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
	ScenarioID    string        `json:"scenario_id"`
	Variant       string        `json:"variant"`
	Query         string        `json:"query"`
	ExecutionTime time.Duration `json:"execution_time"`
	PlanType      string        `json:"plan_type"`
	PlanDetails   string        `json:"plan_details"`
	RowsReturned  int64         `json:"rows_returned"`
	ExplainOnly   bool          `json:"explain_only"`
}

// GetTestScenariosWithRowCountsAndSelectivities converts comprehensive tests to TestScenario format with custom row counts and selectivities
func GetTestScenariosWithRowCountsAndSelectivities(rowCounts []int, selectivities []float64, repetitions int) []TestScenario {
	var scenarios []TestScenario

	// Generate tests for each combination of row count and selectivity
	for _, rowCount := range rowCounts {
		tableSizeName := formatRowCountName(rowCount)

		for _, sel := range selectivities {
			// Calculate the actual value to search for based on selectivity type
			var searchValue int
			if sel <= 1.0 {
				// Percentage-based selectivity (0.0 to 1.0)
				searchValue = int(float64(rowCount) * sel)
			} else {
				// Row count-based selectivity
				searchValue = int(sel)
			}

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

	return scenarios
}

// formatSelectivityName formats a selectivity value into a scenario ID format
func formatSelectivityName(r int, v float64) string {
	// Convert to a safe format for scenario IDs
	if v < 1.0 {
		return fmt.Sprintf("%d", int(v*float64(r)))
	} else {
		// Row count-based: use the row count directly
		return fmt.Sprintf("%d", int(v))
	}
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
