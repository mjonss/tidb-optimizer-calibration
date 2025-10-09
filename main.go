package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ruRegexStr = `(?:"ru_consumption":)(\d+\.\d+)[^\d]`
)

func main() {
	// Parse command line flags
	var logLevel = flag.String("l", "info", "Log level: debug, info, warn, error")
	var rowCounts = flag.String("s", "1K,1M", "Comma-separated list of table sizes to test (e.g., 1,100,10000)")
	var fillerSize = flag.Int("f", 100, "Filler column size")
	var selectivities = flag.String("c", "50.0,25.0,12.5,6.25,3.125,1.5625,0.78125,0.390625,0.1953125", "Comma-separated list of selectivity/cardinality values (Selectivity: ratio (0.0-1.0) or Cardinality: row counts. E.g., 0.3,0.1,100,50,25)")
	var repetitions = flag.Int("n", 1, "Number of times to repeat each test")
	var detailedOutput = flag.Bool("d", true, "Detailed output, one line per test run")
	var aggregatedOutput = flag.Bool("a", false, "Aggregated output, per test")

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

	slog.Debug("Row counts to test", "rows", rows)
	slog.Debug("Selectivity values to test", "selectivities", selValues)

	err = CheckAndSetupTables(rows, selValues, *fillerSize)
	// Run comprehensive optimizer tests
	results := RunOptimizerTests(rows, selValues, *repetitions)
	if err != nil {
		slog.Error("Failed to run optimizer tests", "error", err)
		os.Exit(1)
	}

	if *detailedOutput {
		outputDetailedResultsTable(results)
	}
	if *aggregatedOutput {
		outputAggregatedResultsTable(results)
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

		// Try to parse as float (ratio)
		if ratio, err := strconv.ParseFloat(part, 64); err == nil {
			if ratio <= 0.0 || ratio >= 1.0 {
				return nil, fmt.Errorf("ratio must be between 0 and 1.0, got %f", ratio)
			}
			selectivities = append(selectivities, ratio)
			continue
		}

		return nil, fmt.Errorf("invalid selectivity value '%s': must be a ratio (0-1.0) or positive integer", part)
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
func RunOptimizerTests(rowCounts []int, selectivities []float64, repetitions int) []*TestExecutionResult {
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

	// Run all test combinations against real TiDB cluster
	fmt.Println("\n🎯 Running All Test Combinations Against Real TiDB")
	fmt.Println("================================================")

	// Run all test combinations with real execution
	return runAllTestCombinations(scenarios)
}

// runAllTestCombinations runs all test combinations against a real TiDB cluster
func runAllTestCombinations(scenarios []TestScenario) []*TestExecutionResult {

	slog.Info("Connecting to TiDB cluster", "scenarios", len(scenarios))
	fmt.Printf("Connecting to TiDB cluster and executing %d test scenarios...\n", len(scenarios))
	fmt.Println()

	client := NewTiDBClient()

	err := client.Connect(nil)
	if err != nil {
		slog.Error("Failed to connect to TiDB", "error", err)
		fmt.Printf("❌ Failed to connect to TiDB: %v\n", err)
		fmt.Println("Please ensure TiDB is running on localhost:4000")
		fmt.Println("You can start TiDB with: tiup playground")
		return nil
	}
	defer client.Close()

	slog.Info("Connected to TiDB cluster successfully")
	fmt.Println("✅ Connected to TiDB cluster successfully!")
	fmt.Println()

	// Run all scenarios with repetitions and collect results
	var results []*TestExecutionResult
	totalScenarios := len(scenarios)
	completed := 0

	for _, scenario := range scenarios {
		if completed%10 == 0 {
			fmt.Printf("Progress: %d/%d scenarios completed\n", completed, totalScenarios)
		}
		completed++

		slog.Debug("Executing scenario", "id", scenario.ID, "query", scenario.Query)

		// Execute real test with actual TiDB and capture actual execution plan
		result, err := client.ExecuteQueryWithMetrics(scenario)
		if err != nil {
			fmt.Printf("❌ Error running scenario %s: %v\n", scenario.ID, err)
			continue
		} else {
			slog.Debug("Scenario completed", "scenario_id", scenario.ID, "plan_type", result.PlanType)
		}
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ScenarioID < results[j].ScenarioID
	})

	// Output results in table format
	return results
}

func getRU(plan *ExecutionPlan) float64 {
	if plan == nil {
		return 0.0
	}
	if plan.QueryInfo == "" {
		return 0.0
	}
	ruRegex := regexp.MustCompile(ruRegexStr)
	ruMatch := ruRegex.FindStringSubmatch(plan.QueryInfo)
	if len(ruMatch) == 2 {
		ru, err := strconv.ParseFloat(ruMatch[1], 64)
		if err == nil {
			return ru
		}
	}
	return 0.0
}

// outputResultsTable outputs results in a formatted table
func outputDetailedResultsTable(results []*TestExecutionResult) {
	fmt.Println("\n📊 Test Results Table - All results")
	fmt.Println("====================")

	planChoosen := make(map[string]int)
	fmt.Printf("Scenario\tTable_size\tCardinality\tVariant\tPlan\t")
	fmt.Printf("RU\tms\n")
	// Group results by ScenarioID
	for _, r := range results {
		if r.ExplainOnly {
			planChoosen[r.ScenarioID+"/"+r.PlanType]++
			continue
		}
		scenParts := strings.Split(r.ScenarioID, "_")
		fmt.Printf("%s\t", strings.Join(scenParts, "\t"))
		fmt.Printf("%s\t", r.Variant)
		fmt.Printf("%s\t", r.PlanType)
		fmt.Printf("%.03f\t", getRU(r.Plan))
		fmt.Printf("%.03f\n", r.Plan.ExecutionTime.Seconds()*1000.0)
	}
	fmt.Printf("\nScenario\tTable_size\tCardinality\t")
	fmt.Printf("Plan\tCount\n")
	for k, v := range planChoosen {
		s := strings.Split(k, "/")
		scenParts := strings.Split(s[0], "_")
		fmt.Printf("%s\t%s\t%d\n", strings.Join(scenParts, "\t"), s[1], v)
	}
}

func outputAggregatedResultsTable(results []*TestExecutionResult) {
	fmt.Println("\n📊 Test Results Table - Grouped by test")
	fmt.Println("====================")

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

	for i, scenarioID := range scenarioIDs {
		group := scenarioMap[scenarioID]

		// Collect distinct plan types and stats
		planTypeSum := make(map[string]time.Duration)
		planTypeMin := make(map[string]time.Duration)
		planTypeMax := make(map[string]time.Duration)
		RUSum := make(map[string]float64)
		RUMin := make(map[string]float64)
		RUMax := make(map[string]float64)
		planTypeCount := make(map[string]int)
		explainOnlyPlanType := ""
		for _, res := range group {
			if res.ExplainOnly {
				explainOnlyPlanType = res.PlanType
				continue
			}
			ru := getRU(res.Plan)
			if minimum, ok := RUMin[res.PlanType]; !ok || minimum > ru {
				RUMin[res.PlanType] = ru
			}
			RUSum[res.PlanType] += ru
			if ru > RUMax[res.PlanType] {
				RUMax[res.PlanType] = ru
			}
			t := res.Plan.ExecutionTime
			if minimum, ok := planTypeMin[res.PlanType]; !ok || minimum > t {
				planTypeMin[res.PlanType] = t
			}
			planTypeSum[res.PlanType] += t
			if t > planTypeMax[res.PlanType] {
				planTypeMax[res.PlanType] = t
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
		avgRU := make(map[string]float64)
		for pt, ruSum := range RUSum {
			avgRU[pt] = ruSum / float64(planTypeCount[pt])
		}
		avgTimes := make(map[string]float64)
		for pt, times := range planTypeSum {
			avgTimes[pt] = times.Seconds() / float64(planTypeCount[pt])
		}

		// Print header for this scenario
		if i == 0 {
			fmt.Printf("Scenario\t")
			fmt.Printf("Table size\t")
			fmt.Printf("Cardinality\t")
			fmt.Printf("Choosen\t")
			for i, pt := range planTypes {
				fmt.Printf("%s-ru-min\t", pt)
				fmt.Printf("%s-ru-avg\t", pt)
				fmt.Printf("%s-ru-max\t", pt)
				fmt.Printf("%s-min\t", pt)
				fmt.Printf("%s-avg\t", pt)
				fmt.Printf("%s-max", pt)
				if i == len(planTypes)-1 {
					fmt.Printf("\n")
				} else {
					fmt.Printf("\t")
				}
			}
		}
		scenParts := strings.Split(scenarioID, "_")
		fmt.Printf("%s\t", scenParts[0])
		fmt.Printf("%s\t", scenParts[1])
		fmt.Printf("%s\t", scenParts[2])
		fmt.Printf("%s\t", explainOnlyPlanType)
		for i, pt := range planTypes {
			fmt.Printf("%.03f\t", RUMin[pt])
			fmt.Printf("%.03f\t", avgRU[pt])
			fmt.Printf("%.03f\t", RUMax[pt])
			fmt.Printf("%.03f\t", float64(planTypeMin[pt].Microseconds())/1000.0)
			fmt.Printf("%.03f\t", avgTimes[pt]*1000)
			fmt.Printf("%.03f", float64(planTypeMax[pt].Microseconds())/1000.0)
			if i == len(planTypes)-1 {
				fmt.Printf("\n")
			} else {
				fmt.Printf("\t")
			}
		}
	}
}
