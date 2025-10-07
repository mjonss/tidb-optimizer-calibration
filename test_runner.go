package main

// TestRunner runs actual tests and outputs results
type TestRunner struct {
	client        *TiDBClient
	tableCache    map[string]bool // Cache of created tables to avoid recreation
	selectivities []float64       // Selectivities for data generation
}

// NewTestRunner creates a new test runner
func NewTestRunner(client *TiDBClient, selectivities []float64) *TestRunner {
	return &TestRunner{
		client:        client,
		tableCache:    make(map[string]bool),
		selectivities: selectivities,
	}
}

// RunScenarioWithActualPlan runs a test scenario and captures the actual execution plan
func (tr *TestRunner) RunScenarioWithActualPlan(scenario TestScenario) (*TestExecutionResult, error) {
	// Check if table already exists in cache
	if !tr.tableCache[scenario.TableName] {
		// Setup table with single schema structure (only if not cached)
		runner := NewScenarioRunner(tr.client)
		err := runner.setupTableWithData(scenario.TableName, scenario.RowCount, tr.selectivities)
		if err != nil {
			return nil, err
		}

		// Mark table as created in cache
		tr.tableCache[scenario.TableName] = true
		//} else {
		//	fmt.Printf("♻️  Reusing existing table %s\n", scenario.TableName)
	}

	// Execute query with real metrics
	return tr.client.ExecuteQueryWithMetrics(scenario)
}
