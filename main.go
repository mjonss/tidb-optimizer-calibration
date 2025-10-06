package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn       = flag.String("dsn", "root@tcp(127.0.0.1:4000)/test", "TiDB DSN (Data Source Name)")
	createTbl = flag.Bool("create-table", false, "Create test table")
	testOptim = flag.Bool("test-optimizer", false, "Run optimizer tests")
)

func main() {
	flag.Parse()

	if !*createTbl && !*testOptim {
		fmt.Println("Usage: specify either -create-table or -test-optimizer")
		flag.PrintDefaults()
		os.Exit(1)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if *createTbl {
		if err := createTestTable(db); err != nil {
			log.Fatalf("Failed to create test table: %v", err)
		}
		fmt.Println("Test table created successfully")
	}

	if *testOptim {
		if err := runOptimizerTests(db); err != nil {
			log.Fatalf("Failed to run optimizer tests: %v", err)
		}
	}
}

func createTestTable(db *sql.DB) error {
	// Drop table if exists
	_, err := db.Exec("DROP TABLE IF EXISTS t")
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Create test table with specified schema
	createSQL := `CREATE TABLE t (
		a BIGINT AUTO_INCREMENT PRIMARY KEY,
		b INT,
		c VARCHAR(255),
		KEY idx_b (b),
		KEY idx_cb (c, b)
	)`

	_, err = db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert test data
	fmt.Println("Inserting test data...")
	for i := 1; i <= 10000; i++ {
		_, err = db.Exec("INSERT INTO t (b, c) VALUES (?, ?)", i%100, fmt.Sprintf("value_%d", i%50))
		if err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
		}
		if i%1000 == 0 {
			fmt.Printf("Inserted %d rows...\n", i)
		}
	}

	// Analyze table to update statistics
	_, err = db.Exec("ANALYZE TABLE t")
	if err != nil {
		return fmt.Errorf("failed to analyze table: %w", err)
	}

	fmt.Println("Table created and populated with 10000 rows")
	return nil
}

func runOptimizerTests(db *sql.DB) error {
	fmt.Println("\n=== Running Optimizer Tests ===")

	tests := []struct {
		name        string
		query       string
		description string
	}{
		{
			name:        "Test 1: Full table scan (SELECT *)",
			query:       "SELECT * FROM t",
			description: "Should use TableFullScan as we need all columns",
		},
		{
			name:        "Test 2: Primary key point query",
			query:       "SELECT * FROM t WHERE a = 100",
			description: "Should use Point_Get on primary key",
		},
		{
			name:        "Test 3: Primary key range query",
			query:       "SELECT * FROM t WHERE a >= 100 AND a <= 200",
			description: "Should use TableRangeScan on primary key",
		},
		{
			name:        "Test 4: Index b point query",
			query:       "SELECT * FROM t WHERE b = 10",
			description: "Should use IndexLookUp on idx_b (needs table lookup for column c)",
		},
		{
			name:        "Test 5: Index b covering query",
			query:       "SELECT a, b FROM t WHERE b = 10",
			description: "Should use IndexReader on idx_b (covering index with primary key)",
		},
		{
			name:        "Test 6: Composite index query",
			query:       "SELECT * FROM t WHERE c = 'value_10'",
			description: "Should use IndexLookUp on idx_cb (c is first column of composite index)",
		},
		{
			name:        "Test 7: Composite index covering query",
			query:       "SELECT c, b FROM t WHERE c = 'value_10'",
			description: "Should use IndexReader on idx_cb (covering index)",
		},
		{
			name:        "Test 8: Non-selective query",
			query:       "SELECT * FROM t WHERE b > 50",
			description: "Might use TableFullScan if selectivity is low",
		},
		{
			name:        "Test 9: Highly selective query",
			query:       "SELECT * FROM t WHERE b = 1",
			description: "Should use IndexLookUp on idx_b (high selectivity)",
		},
	}

	for _, test := range tests {
		fmt.Printf("--- %s ---\n", test.name)
		fmt.Printf("Query: %s\n", test.query)
		fmt.Printf("Description: %s\n", test.description)

		// Get execution plan
		explainQuery := "EXPLAIN " + test.query
		rows, err := db.Query(explainQuery)
		if err != nil {
			return fmt.Errorf("failed to explain query: %w", err)
		}

		fmt.Println("\nExecution Plan:")
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Print header
		for i, col := range cols {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(col)
		}
		fmt.Println()

		// Print rows
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row: %w", err)
			}

			for i, val := range values {
				if i > 0 {
					fmt.Print("\t")
				}
				if val != nil {
					fmt.Print(val)
				} else {
					fmt.Print("NULL")
				}
			}
			fmt.Println()
		}
		rows.Close()

		// Analyze the plan
		analyzeQuery := "EXPLAIN ANALYZE " + test.query
		rows, err = db.Query(analyzeQuery)
		if err != nil {
			return fmt.Errorf("failed to analyze query: %w", err)
		}

		fmt.Println("\nExecution Analysis:")
		cols, err = rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Print header
		for i, col := range cols {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(col)
		}
		fmt.Println()

		// Print rows
		values = make([]interface{}, len(cols))
		valuePtrs = make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row: %w", err)
			}

			for i, val := range values {
				if i > 0 {
					fmt.Print("\t")
				}
				if val != nil {
					fmt.Print(val)
				} else {
					fmt.Print("NULL")
				}
			}
			fmt.Println()
		}
		rows.Close()

		fmt.Println("\n" + strings.Repeat("=", 80) + "\n")
	}

	return nil
}
