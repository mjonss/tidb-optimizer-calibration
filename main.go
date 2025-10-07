package main

import (
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	// Parse command line flags
	var logLevel = flag.String("log-level", "info", "Log level: debug, info, warn, error")
	var rowCounts = flag.String("rows", "1,10,100,1K,10K,100K,1M,10M", "Comma-separated list of row counts to test (e.g., 1,100,10000)")
	var selectivities = flag.String("selectivity", "50.0,25.0,12.5,6.25,3.125,1.5625,0.78125,0.390625,0.1953125", "Comma-separated list of selectivity values (percentages or row counts, e.g., 50,25,12.5 or 100,50,25)")
	var repetitions = flag.Int("repetitions", 1, "Number of times to repeat each scenario")
	flag.Parse()

	// Set up structured logging with slog
	setupLogging(*logLevel)

	// Parse row counts
	rows, err := parseRowCounts(*rowCounts)
	if err != nil {
		slog.Error("Invalid row counts", "error", err)
		os.Exit(1)
	}

	// Parse selectivities
	selValues, err := parseSelectivities(*selectivities)
	if err != nil {
		slog.Error("Invalid selectivities", "error", err)
		os.Exit(1)
	}

	// Run TiDB Optimizer Calibration Tool
	slog.Info("Starting TiDB Optimizer Calibration Tool")
	slog.Info("======================================")
	slog.Info("Row counts to test", "rows", rows)
	slog.Info("Selectivity values to test", "selectivities", selValues)

	// Run comprehensive optimizer tests
	err = RunOptimizerTests(rows, selValues, *repetitions)
	if err != nil {
		slog.Error("Failed to run optimizer tests", "error", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TiDB Optimizer Calibration completed successfully!")
}

// parseRowCounts parses comma-separated row counts from command line
func parseRowCounts(rowCountsStr string) ([]int, error) {
	if rowCountsStr == "" {
		return []int{}, fmt.Errorf("row counts cannot be empty")
	}

	parts := strings.Split(rowCountsStr, ",")
	rows := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Handle k/K, M, G suffixes for row counts
		multiplier := 1
		lower := strings.ToLower(part[len(part)-1:])
		switch lower {
		case "k":
			multiplier = 1000
			part = part[:len(part)-1]
		case "m":
			multiplier = 1000000
			part = part[:len(part)-1]
		case "g":
			multiplier = 1000000000
			part = part[:len(part)-1]
		}

		rowCount, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid row count '%s': %w", part, err)
		}

		if rowCount <= 0 {
			return nil, fmt.Errorf("row count must be positive, got %d", rowCount)
		}

		rows = append(rows, rowCount*multiplier)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("no valid row counts provided")
	}

	return rows, nil
}

// parseSelectivities parses comma-separated selectivity values from command line
func parseSelectivities(selectivitiesStr string) ([]float64, error) {
	if selectivitiesStr == "" {
		return nil, fmt.Errorf("selectivities cannot be empty")
	}

	parts := strings.Split(selectivitiesStr, ",")
	selectivities := make([]float64, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Try to parse as integer first (row count)
		if rowCount, err := strconv.Atoi(part); err == nil {
			if rowCount <= 0 {
				return nil, fmt.Errorf("row count must be positive, got %d", rowCount)
			}
			selectivities = append(selectivities, float64(rowCount))
			continue
		}

		// Try to parse as float (percentage)
		if percentage, err := strconv.ParseFloat(part, 64); err == nil {
			if percentage <= 0 || percentage > 100 {
				return nil, fmt.Errorf("percentage must be between 0 and 100, got %f", percentage)
			}
			selectivities = append(selectivities, percentage/100.0)
			continue
		}

		return nil, fmt.Errorf("invalid selectivity value '%s': must be percentage (0-100) or positive integer", part)
	}

	if len(selectivities) == 0 {
		return nil, fmt.Errorf("no valid selectivity values provided")
	}

	return selectivities, nil
}

// formatRowCount formats a row count into a human-readable string
func formatRowCount(count int) string {
	switch {
	case count >= 1000000:
		return fmt.Sprintf("%dM", count/1000000)
	case count >= 1000:
		return fmt.Sprintf("%dK", count/1000)
	default:
		return fmt.Sprintf("%d", count)
	}
}

// sortTableSizes sorts table sizes by their numeric value
func sortTableSizes(tableSizes []string) {
	sort.Slice(tableSizes, func(i, j int) bool {
		// Convert table size strings to numeric values for comparison
		valI := parseTableSizeToNumber(tableSizes[i])
		valJ := parseTableSizeToNumber(tableSizes[j])
		return valI < valJ
	})
}

// parseTableSizeToNumber converts table size string to numeric value for sorting
func parseTableSizeToNumber(tableSize string) int {
	// Try to parse as integer first
	if count, err := strconv.Atoi(tableSize); err == nil {
		return count
	}

	// Handle K/M suffixes
	if strings.HasSuffix(tableSize, "K") {
		if count, err := strconv.Atoi(strings.TrimSuffix(tableSize, "K")); err == nil {
			return count * 1000
		}
	}

	if strings.HasSuffix(tableSize, "M") {
		if count, err := strconv.Atoi(strings.TrimSuffix(tableSize, "M")); err == nil {
			return count * 1000000
		}
	}

	return 0
}

// setupLogging configures structured logging with the specified level
func setupLogging(level string) {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	// Create a handler with the specified level
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})

	// Set the default logger
	slog.SetDefault(slog.New(handler))
}

// RunOptimizerTests runs comprehensive optimizer calibration tests
func RunOptimizerTests(rowCounts []int, selectivities []float64, repetitions int) error {
	slog.Info("Running TiDB Optimizer Calibration Tests")
	slog.Info("======================================")

	// Get comprehensive test scenarios with custom row counts and selectivities
	scenarios := GetTestScenariosWithRowCountsAndSelectivities(rowCounts, selectivities, repetitions)

	fmt.Printf("\n📋 Test Suite Overview: %d comprehensive scenarios\n", len(scenarios))
	fmt.Println("Focus: Index Lookup vs Table Scan decisions")

	// Display row counts in a readable format
	rowCountStrs := make([]string, len(rowCounts))
	for i, count := range rowCounts {
		rowCountStrs[i] = formatRowCount(count)
	}
	fmt.Printf("Data sizes: %s rows\n", strings.Join(rowCountStrs, ", "))

	// Display selectivities in a readable format
	selStrs := make([]string, len(selectivities))
	for i, sel := range selectivities {
		selStrs[i] = strconv.Itoa(int(sel))
	}
	fmt.Printf("Selectivity: %s\n", strings.Join(selStrs, ", "))
	fmt.Printf("Table structure: "+IndexVsTableSchemaFmt, "t1K")

	// Run all test combinations against real TiDB cluster
	fmt.Println("\n🎯 Running All Test Combinations Against Real TiDB")
	fmt.Println("================================================")

	// Run all test combinations with real execution
	runAllTestCombinations(scenarios, selectivities, repetitions)

	return nil
}

// runAllTestCombinations runs all test combinations against a real TiDB cluster
func runAllTestCombinations(scenarios []TestScenario, selectivities []float64, repetitions int) {

	slog.Info("Connecting to TiDB cluster", "scenarios", len(scenarios))
	fmt.Printf("Connecting to TiDB cluster and executing %d test scenarios...\n", len(scenarios))
	fmt.Println()

	// Connect to TiDB
	config := TiDBConfig{
		Host:     "localhost",
		Port:     4000,
		User:     "root",
		Password: "",
		Database: "test",
		Timeout:  30 * time.Second,
	}

	slog.Debug("TiDB connection config", "host", config.Host, "port", config.Port, "database", config.Database)
	client := NewTiDBClient()

	err := client.Connect(config)
	if err != nil {
		slog.Error("Failed to connect to TiDB", "error", err)
		fmt.Printf("❌ Failed to connect to TiDB: %v\n", err)
		fmt.Println("Please ensure TiDB is running on localhost:4000")
		fmt.Println("You can start TiDB with: tiup playground")
		return
	}
	defer client.Close()

	slog.Info("Connected to TiDB cluster successfully")
	fmt.Println("✅ Connected to TiDB cluster successfully!")
	fmt.Println()

	// Create test runner with the connected client and selectivities
	testRunner := NewTestRunner(client, selectivities)

	rand.Shuffle(len(scenarios), func(i, j int) {
		scenarios[i], scenarios[j] = scenarios[j], scenarios[i]
	})

	// Run all scenarios with repetitions and collect results
	var results []*TestExecutionResult
	totalScenarios := len(scenarios) * repetitions
	completed := 0

	for _, scenario := range scenarios {
		if completed%10 == 0 {
			slog.Info("Test progress", "completed", completed, "total", totalScenarios)
			fmt.Printf("Progress: %d/%d scenarios completed\n", completed, totalScenarios)
		}
		completed++

		slog.Debug("Executing scenario", "id", scenario.ID, "query", scenario.Query)

		// Execute real test with actual TiDB and capture actual execution plan
		result, err := testRunner.RunScenarioWithActualPlan(scenario)
		if err != nil {
			slog.Error("Error running scenario", "scenario_id", scenario.ID, "error", err)
			fmt.Printf("❌ Error running scenario %s: %v\n", scenario.ID, err)
			continue
		} else {
			slog.Debug("Scenario completed", "scenario_id", scenario.ID, "plan_type", result.PlanType, "execution_time", result.ExecutionTime)
		}
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ScenarioID < results[j].ScenarioID
	})

	// Output results in table format
	outputResultsTable(results)

	// Output compact summary table
	outputCompactSummaryTable(results)

	// Output summary statistics
	outputSummaryStatistics(results)
}

// outputResultsTable outputs results in a formatted table
func outputResultsTable(results []*TestExecutionResult) {
	fmt.Println("\n📊 Test Results Table")
	fmt.Println("====================")

	// Group results by ScenarioID
	scenarioMap := make(map[string][]*TestExecutionResult)
	for _, result := range results {
		scenarioMap[result.ScenarioID] = append(scenarioMap[result.ScenarioID], result)
	}

	// For deterministic output, get sorted ScenarioIDs
	var scenarioIDs []string
	for scenarioID := range scenarioMap {
		scenarioIDs = append(scenarioIDs, scenarioID)
	}
	sort.Strings(scenarioIDs)

	for _, scenarioID := range scenarioIDs {
		group := scenarioMap[scenarioID]

		// Collect distinct plan types and stats
		planTypeSum := make(map[string]time.Duration)
		planTypeMin := make(map[string]time.Duration)
		planTypeMax := make(map[string]time.Duration)
		planTypeCount := make(map[string]int)
		explainOnlyPlanType := ""
		for _, res := range group {
			if res.ExplainOnly {
				explainOnlyPlanType = res.PlanType
				continue
			}
			if minimum, ok := planTypeMin[res.PlanType]; !ok || minimum > res.ExecutionTime {
				planTypeMin[res.PlanType] = res.ExecutionTime
			}
			planTypeSum[res.PlanType] += res.ExecutionTime
			if res.ExecutionTime > planTypeMax[res.PlanType] {
				planTypeMax[res.PlanType] = res.ExecutionTime
			}
			planTypeCount[res.PlanType]++
		}

		if _, ok := planTypeSum[explainOnlyPlanType]; !ok {
			slog.Error("Actual optimizer choice not tested!!!", "plan_type", explainOnlyPlanType)
		}
		// Get sorted plan types
		var planTypes []string
		for pt := range planTypeSum {
			planTypes = append(planTypes, pt)
		}
		sort.Strings(planTypes)

		// Compute average time per plan type
		avgTimes := make(map[string]float64)
		for pt, times := range planTypeSum {
			avgTimes[pt] = times.Seconds() / float64(planTypeCount[pt])
		}

		// Print header for this scenario
		fmt.Printf("\nScenario: %s\n", scenarioID)
		fmt.Printf("  Plan Types: %s\n", strings.Join(planTypes, ", "))
		fmt.Printf("  ExplainOnly Plan Type: %s\n", explainOnlyPlanType)
		fmt.Printf("  Average Time per Plan Type:\n")
		for _, pt := range planTypes {
			fmt.Printf("    %s: %.03f ms\n", pt, avgTimes[pt]*1000)
		}
		fmt.Printf("  Max Time per Plan Type:\n")
		for _, pt := range planTypes {
			fmt.Printf("    %s: %s\n", pt, planTypeMax[pt].String())
		}
	}
	// Table header
	fmt.Printf("%-20s %-15s %-12s %-8s %-8s %-8s %-8s %-8s\n",
		"Scenario", "Plan Type", "Query Type", "Time(ms)", "RU", "Rows", "Cost", "Table")
	fmt.Println(strings.Repeat("-", 100))

	// Group results by table size for better organization
	// Extract unique table sizes from results
	tableSizeMap := make(map[string]bool)
	for _, result := range results {
		// Extract table size from scenario ID (e.g., "index_100K_1/2" -> "100K")
		parts := strings.Split(result.ScenarioID, "_")
		if len(parts) >= 2 {
			tableSizeMap[parts[1]] = true
		}
	}

	// Convert map to slice and sort
	var tableSizes []string
	for tableSize := range tableSizeMap {
		tableSizes = append(tableSizes, tableSize)
	}

	// Sort table sizes by their numeric value
	sortTableSizes(tableSizes)

	for _, tableSize := range tableSizes {
		fmt.Printf("\n📋 Table Size: %s rows\n", tableSize)
		fmt.Println(strings.Repeat("-", 100))

		// Filter results for this table size
		var tableResults []*TestExecutionResult
		for _, result := range results {
			if strings.Contains(result.ScenarioID, tableSize) {
				tableResults = append(tableResults, result)
			}
		}

		// Sort by selectivity (1/2 to 1/512)
		selectivities := []string{"1/2", "1/4", "1/8", "1/16", "1/32", "1/64", "1/128", "1/256", "1/512"}

		for _, sel := range selectivities {
			// Find index lookup and table scan for this selectivity
			var indexResult, tableScanResult *TestExecutionResult
			for _, result := range tableResults {
				if strings.Contains(result.ScenarioID, sel) {
					if strings.Contains(result.ScenarioID, "index_") {
						indexResult = result
					} else if strings.Contains(result.ScenarioID, "tablescan_") {
						tableScanResult = result
					}
				}
			}

			// Output index lookup result
			if indexResult != nil {
				queryType := "Index"
				timeMs := float64(indexResult.ExecutionTime.Nanoseconds()) / 1000000.0
				fmt.Printf("%-20s %-15s %-12s %-8.1f %-8d %-8s\n",
					indexResult.ScenarioID, indexResult.PlanType, queryType,
					timeMs, indexResult.RowsReturned,
					tableSize)
			}

			// Output table scan result
			if tableScanResult != nil {
				queryType := "TableScan"
				timeMs := float64(tableScanResult.ExecutionTime.Nanoseconds()) / 1000000.0
				fmt.Printf("%-20s %-15s %-12s %-8.1f %-8d %-8s\n",
					tableScanResult.ScenarioID, tableScanResult.PlanType, queryType,
					timeMs, tableScanResult.RowsReturned,
					tableSize)
			}
		}
	}
}

// outputCompactSummaryTable outputs a compact summary table
func outputCompactSummaryTable(results []*TestExecutionResult) {
	fmt.Println("\n📋 Compact Summary Table")
	fmt.Println("=======================")

	// Group by table size and show average performance
	// Extract unique table sizes from results
	tableSizeMap := make(map[string]bool)
	for _, result := range results {
		// Extract table size from scenario ID (e.g., "index_100K_1/2" -> "100K")
		parts := strings.Split(result.ScenarioID, "_")
		if len(parts) >= 2 {
			tableSizeMap[parts[1]] = true
		}
	}

	// Convert map to slice and sort
	var tableSizes []string
	for tableSize := range tableSizeMap {
		tableSizes = append(tableSizes, tableSize)
	}

	// Sort table sizes by their numeric value
	sortTableSizes(tableSizes)

	fmt.Printf("%-8s %-12s %-12s %-12s %-12s %-12s %-12s\n",
		"Table", "Index Time", "Index RU", "Scan Time", "Scan RU", "Index Rows", "Scan Rows")
	fmt.Println(strings.Repeat("-", 80))

	for _, tableSize := range tableSizes {
		// Calculate averages for this table size
		var indexTime, scanTime time.Duration
		var indexRU, scanRU float64
		var indexRows, scanRows int64
		var indexCount, scanCount int

		for _, result := range results {
			if strings.Contains(result.ScenarioID, tableSize) {
				if result.PlanType == "index_lookup" {
					indexTime += result.ExecutionTime
					indexRows += result.RowsReturned
					indexCount++
				} else if result.PlanType == "table_scan" {
					scanTime += result.ExecutionTime
					scanRows += result.RowsReturned
					scanCount++
				}
			}
		}

		// Calculate averages
		var avgIndexTime, avgScanTime time.Duration
		var avgIndexRU, avgScanRU float64
		var avgIndexRows, avgScanRows float64

		if indexCount > 0 {
			avgIndexTime = indexTime / time.Duration(indexCount)
			avgIndexRU = indexRU / float64(indexCount)
			avgIndexRows = float64(indexRows) / float64(indexCount)
		}

		if scanCount > 0 {
			avgScanTime = scanTime / time.Duration(scanCount)
			avgScanRU = scanRU / float64(scanCount)
			avgScanRows = float64(scanRows) / float64(scanCount)
		}

		// Format output
		indexTimeMs := float64(avgIndexTime.Nanoseconds()) / 1000000.0
		scanTimeMs := float64(avgScanTime.Nanoseconds()) / 1000000.0

		fmt.Printf("%-8s %-12.1f %-12.2f %-12.1f %-12.2f %-12.0f %-12.0f\n",
			tableSize, indexTimeMs, avgIndexRU, scanTimeMs, avgScanRU, avgIndexRows, avgScanRows)
	}
}

// outputSummaryStatistics outputs summary statistics
func outputSummaryStatistics(results []*TestExecutionResult) {
	fmt.Println("\n📈 Summary Statistics")
	fmt.Println("====================")

	var totalTime time.Duration
	var indexLookups, tableScans int
	var totalRows int64
	var totalCost float64

	for _, result := range results {
		totalTime += result.ExecutionTime
		totalRows += result.RowsReturned

		if result.PlanType == "index_lookup" {
			indexLookups++
		} else if result.PlanType == "table_scan" {
			tableScans++
		}
	}

	if len(results) == 0 {
		fmt.Println("No results found")
		return
	}
	avgTime := totalTime / time.Duration(len(results))
	avgRows := float64(totalRows) / float64(len(results))
	avgCost := totalCost / float64(len(results))

	fmt.Printf("Total Tests Executed: %d\n", len(results))
	fmt.Printf("Total Execution Time: %v\n", totalTime)
	fmt.Printf("Average Execution Time: %v\n", avgTime)
	fmt.Printf("Total Rows Returned: %d\n", totalRows)
	fmt.Printf("Average Rows per Query: %.1f\n", avgRows)
	fmt.Printf("Total Cost: %.2f\n", totalCost)
	fmt.Printf("Average Cost: %.2f\n", avgCost)
	fmt.Printf("Index Lookups: %d (%.1f%%)\n", indexLookups, float64(indexLookups)/float64(len(results))*100)
	fmt.Printf("Table Scans: %d (%.1f%%)\n", tableScans, float64(tableScans)/float64(len(results))*100)

	// Performance comparison
	fmt.Println("\n⚡ Performance Analysis")
	fmt.Println("======================")

	// Calculate average performance by plan type
	var indexTime, tableScanTime time.Duration
	var indexRU, tableScanRU float64
	var indexCount, tableScanCount int

	for _, result := range results {
		if result.PlanType == "index_lookup" {
			indexTime += result.ExecutionTime
			indexCount++
		} else if result.PlanType == "table_scan" {
			tableScanTime += result.ExecutionTime
			tableScanCount++
		}
	}

	if indexCount > 0 {
		avgIndexTime := indexTime / time.Duration(indexCount)
		avgIndexRU := indexRU / float64(indexCount)
		fmt.Printf("Average Index Lookup Time: %v\n", avgIndexTime)
		fmt.Printf("Average Index Lookup RU: %.2f\n", avgIndexRU)
	}

	if tableScanCount > 0 {
		avgTableScanTime := tableScanTime / time.Duration(tableScanCount)
		avgTableScanRU := tableScanRU / float64(tableScanCount)
		fmt.Printf("Average Table Scan Time: %v\n", avgTableScanTime)
		fmt.Printf("Average Table Scan RU: %.2f\n", avgTableScanRU)
	}
}
