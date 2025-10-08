package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
)

const (
	IndexVsTableSchemaFmt = "CREATE TABLE %s (id int AUTO_INCREMENT PRIMARY KEY, b int, c varchar(255), KEY (b))"
)

// ScenarioRunner executes test scenarios against TiDB
type ScenarioRunner struct {
	client *TiDBClient
}

// NewScenarioRunner creates a new scenario runner
func NewScenarioRunner(client *TiDBClient) *ScenarioRunner {
	return &ScenarioRunner{
		client: client,
	}
}

// checkTableStatus checks if a table exists and returns its row count
func (r *ScenarioRunner) checkTableStatus(tableName string) (int, error) {
	// Get current row count
	var rowCount int
	slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	err := r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in table: %v", err)
	}

	return rowCount, nil
}

// generateTestData generates test data with varying selectivity patterns
func (r *ScenarioRunner) generateTestData(tableName string, rowCount int, selectivities []float64) error {
	// Check if table exists and has correct number of rows
	recreateTable := false
	currentRowCount, err := r.checkTableStatus(tableName)
	if err != nil {
		recreateTable = true
	}

	if recreateTable {
		slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
		_, err = r.client.ExecuteQuery(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			return fmt.Errorf("failed to clear existing data: %v", err)
		}
		createStmt := fmt.Sprintf(IndexVsTableSchemaFmt, tableName)
		_, err = r.client.ExecuteQuery(createStmt)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	if currentRowCount != rowCount {
		_, err := r.client.ExecuteQuery(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
		if err != nil {
			return fmt.Errorf("failed to clear existing data: %v", err)
		}

		// Generate random data
		err = r.generateRandomData(tableName, rowCount)
		if err != nil {
			return fmt.Errorf("failed to generate random data: %v", err)
		}
	}

	// Adjust selectivities
	err = r.adjustSelectivities(tableName, rowCount, selectivities)
	if err != nil {
		return fmt.Errorf("failed to adjust selectivities: %v", err)
	}

	return nil
}

// generateRandomData generates random data for the table
func (r *ScenarioRunner) generateRandomData(tableName string, rowCount int) error {
	batchSize := 100000
	fmt.Printf("📊 Generating %d rows of random data... (one dot = %d rows)\n", rowCount, batchSize)

	_, err := r.client.ExecuteQuery(fmt.Sprintf("drop table if exists tmp_%s", tableName))
	if err != nil {
		return fmt.Errorf("failed to drop tmp table: %v", err)
	}
	_, err = r.client.ExecuteQuery(fmt.Sprintf("create table tmp_%s (a int primary key)", tableName))
	if err != nil {
		return fmt.Errorf("failed to create tmp table: %v", err)
	}
	tmpInsert := fmt.Sprintf("insert into tmp_%s (a) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)", tableName)
	_, err = r.client.ExecuteQuery(tmpInsert)
	if err != nil {
		return fmt.Errorf("failed to insert random data batch: %v", err)
	}
	tmpTbls := fmt.Sprintf("tmp_%s", tableName)
	for i := 10; i <= batchSize; i++ {
		tmpTbls += fmt.Sprintf(", tmp_%s tt%d", tableName, i)
		i *= 10
	}
	// Keep inserting until we have enough rows
	remainingRows := rowCount
	for {
		// Check current row count
		currentBatchSize := batchSize

		var currentRowCount int
		slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
		err := r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&currentRowCount)
		if err != nil {
			return fmt.Errorf("failed to count current rows: %v", err)
		}
		slog.Debug("Current row count", "currentRowCount", currentRowCount)

		if currentRowCount >= rowCount {
			break
		}

		if remainingRows <= batchSize {
			// Calculate how many more rows we need:w

			remainingRows := rowCount - currentRowCount
			if remainingRows < currentBatchSize {
				currentBatchSize = remainingRows
			}
		}

		// Generate batch insert with random ID and values using INSERT IGNORE
		query := fmt.Sprintf("INSERT IGNORE INTO %s (b,c) SELECT FLOOR(RAND() * 1000000), rand() * 1000000000 FROM %s LIMIT %d", tableName, tmpTbls, currentBatchSize)
		_, err = r.client.ExecuteQuery(query)
		if err != nil {
			return fmt.Errorf("failed to insert random data batch: %v", err)
		}
		remainingRows -= currentBatchSize
		fmt.Printf(".")
	}
	fmt.Printf("\n")
	_, err = r.client.ExecuteQuery(fmt.Sprintf("drop table tmp_%s", tableName))
	if err != nil {
		return fmt.Errorf("failed to drop tmp table: %v", err)
	}

	// Validate that we have the correct number of rows
	var actualRowCount int
	slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	err = r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&actualRowCount)
	if err != nil {
		return fmt.Errorf("failed to count rows: %v", err)
	}

	if actualRowCount != rowCount {
		return fmt.Errorf("expected %d rows, got %d rows", rowCount, actualRowCount)
	}

	slog.Debug("Executing query", "query", fmt.Sprintf("ANALYZE TABLE %s", tableName))
	_, err = r.client.ExecuteQuery(fmt.Sprintf("ANALYZE TABLE %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to count rows: %v", err)
	}
	fmt.Printf("✅ Generated %d rows of random data\n", actualRowCount)
	return nil
}

func getRandomNotInList(l []int) int {
	for {
		ret := rand.Intn(1000000) + 1
		for i := range l {
			if i == ret {
				continue
			}
		}
		return ret
	}
}

// adjustSelectivities adjusts the data to have specific selectivity patterns
func (r *ScenarioRunner) adjustSelectivities(tableName string, rowCount int, selectivities []float64) error {
	batchSize := 50000
	fmt.Printf("🎯 Adjusting selectivities... (one c/+/- is up to %d rows updated)\n", batchSize)

	if len(selectivities) == 0 {
		return errors.New("no selectivities given")
	}
	rowsForSelectivities := make([]int, 0, len(selectivities))

	notIn := ""
	numberOfRowsToUpdate := 0
	for _, sel := range selectivities {
		// Calculate number of rows for this selectivity
		rowsForThisSelectivity := GetNumRows(rowCount, sel)
		rowsForSelectivities = append(rowsForSelectivities, rowsForThisSelectivity)
		numberOfRowsToUpdate += rowsForThisSelectivity
		if len(selectivities) > 1 {
			if notIn != "" {
				notIn += ","
			}
			notIn += fmt.Sprintf("%d", rowsForThisSelectivity)
		}
	}
	if len(selectivities) > 1 {
		notIn = "WHERE b NOT IN (" + notIn + ")"
	}
	if numberOfRowsToUpdate >= rowCount {
		return errors.New("Total selectivity > 100%")
	}

	var actualRowCount int
	for {
		slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s where b <= 0", tableName))
		err := r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s where b <= 0", tableName)).Scan(&actualRowCount)
		if err != nil {
			return fmt.Errorf("failed to count rows: %v", err)
		}
		if actualRowCount == 0 {
			break
		}
		b := getRandomNotInList(rowsForSelectivities)
		_, err = r.client.ExecuteQuery(fmt.Sprintf("UPDATE %s SET b = %d WHERE b <= 0 LIMIT %d", tableName, b, batchSize))
		if err != nil {
			return fmt.Errorf("failed to clean up negative b's: %v", err)
		}
		fmt.Printf("c")

	}

	for i, sel := range selectivities {
		// Calculate number of rows for this selectivity
		rowsForThisSelectivity := rowsForSelectivities[i]

		if rowsForThisSelectivity <= 0 {
			fmt.Printf("  - %d: Skipping (rowsForThisSelectivity <= 0)\n", int(sel))
			continue
		}

		// If already have the correct number of rows, skip
		slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity))
		err := r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity)).Scan(&actualRowCount)
		if err != nil {
			return fmt.Errorf("failed to count rows: %v", err)
		}

		if actualRowCount == rowsForThisSelectivity {
			continue
		}

		// Too many rows
		for actualRowCount > rowsForThisSelectivity {
			b := getRandomNotInList(rowsForSelectivities)
			_, err = r.client.ExecuteQuery(fmt.Sprintf(
				"UPDATE %s SET b = %d WHERE b = %d ORDER BY RAND() LIMIT %d", tableName, b, rowsForThisSelectivity, batchSize))
			if err != nil {
				return fmt.Errorf("failed to decrease matching rows: %v", err)
			}
			slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity))
			err = r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity)).Scan(&actualRowCount)
			if err != nil {
				return fmt.Errorf("failed to count rows: %v", err)
			}
			fmt.Printf("-")
		}

		// Then update the required number of rows to the target value
		for actualRowCount < rowsForThisSelectivity {
			limit := min(rowsForThisSelectivity, batchSize)
			_, err = r.client.ExecuteQuery(fmt.Sprintf(
				"UPDATE %s SET b = %d %s ORDER BY RAND() LIMIT %d",
				tableName, rowsForThisSelectivity, notIn, limit))
			if err != nil {
				return fmt.Errorf("failed to set selectivity %d: %v", int(sel), err)
			}
			slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity))
			err = r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s where b = %d", tableName, rowsForThisSelectivity)).Scan(&actualRowCount)
			if err != nil {
				return fmt.Errorf("failed to count rows: %v", err)
			}
			fmt.Printf("+")
		}
	}
	fmt.Printf("\n")

	for i, sel := range selectivities {
		// Verify the update was successful by counting rows with the target value
		rowsForThisSelectivity := rowsForSelectivities[i]
		var actualRowsWithValue int
		slog.Debug("Executing query", "query", fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE b = %d", tableName, rowsForThisSelectivity))
		err := r.client.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE b = %d", tableName, rowsForThisSelectivity)).Scan(&actualRowsWithValue)
		if err != nil {
			return fmt.Errorf("failed to verify selectivity %f: %v", sel, err)
		}

		if actualRowsWithValue != rowsForThisSelectivity {
			fmt.Printf("  - %f: Warning - Expected %d rows with value %d, got %d\n",
				sel, rowsForThisSelectivity, rowsForThisSelectivity, actualRowsWithValue)
		} else {
			fmt.Printf("  - %f: ✅ %d rows set to value %d (verified)\n", sel, rowsForThisSelectivity, rowsForThisSelectivity)
		}
	}

	_, err := r.client.ExecuteQuery(fmt.Sprintf("ANALYZE TABLE %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to count rows: %v", err)
	}
	fmt.Printf("✅ Selectivity adjustment completed\n")
	return nil
}

// setupTableWithData creates a table with the standard schema and populates it with data
func (r *ScenarioRunner) setupTableWithData(tableName string, rowCount int, selectivities []float64) error {
	// Check if table already exists with correct row count
	err := r.generateTestData(tableName, rowCount, selectivities)
	if err != nil {
		return fmt.Errorf("failed to populate table %s: %w", tableName, err)
	}
	fmt.Printf("✅ Table %s created and populated with %d rows\n", tableName, rowCount)
	return nil
}
