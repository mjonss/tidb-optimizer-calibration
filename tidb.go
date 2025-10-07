package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TiDBClient represents a TiDB database client
type TiDBClient struct {
	db           *sql.DB
	connectionID int
}

type ExecutionPlan struct {
	ID            string         `json:"id"`
	Task          string         `json:"task"`
	Count         int64          `json:"count"`
	EstRows       float64        `json:"estRows"`
	ActRows       int64          `json:"actRows"`
	AccessObject  string         `json:"access object"`
	OperatorInfo  string         `json:"operator info"`
	ExecutionInfo string         `json:"execution info"`
	Memory        string         `json:"memory"`
	Disk          string         `json:"disk"`
	Next          *ExecutionPlan `json:"next,omitempty"`
}

// TiDBConfig holds TiDB connection configuration
type TiDBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Timeout  time.Duration
}

// NewTiDBClient creates a new TiDB client
func NewTiDBClient() *TiDBClient {
	return &TiDBClient{}
}

// Connect establishes a connection to TiDB
func (c *TiDBClient) Connect(config TiDBConfig) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%s&parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database, config.Timeout)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.db = db
	c.connectionID, err = c.getConnectionID()
	if err != nil {
		return fmt.Errorf("failed to get connection ID: %w", err)
	}
	return nil
}

// ExecuteQuery executes a SQL query and returns the result
func (c *TiDBClient) ExecuteQuery(query string) (*sql.Rows, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	slog.Debug("Executing query", "query", query)

	return c.db.Query(query)
}

// GetExecutionPlan returns the execution plan for a query
func (c *TiDBClient) GetExecutionPlan(query string) (*ExecutionPlan, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Use EXPLAIN to get the tabular format execution plan
	explainQuery := fmt.Sprintf("EXPLAIN %s", query)
	slog.Debug("Executing query", "query", explainQuery)
	rows, err := c.db.Query(explainQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution plan: %w", err)
	}
	defer rows.Close()

	// Parse the tabular format execution plan
	return c.parseTabularExecutionPlan(rows)
}

// EXPLAIN that return 5 columns, like normal 'EXPLAIN SELECT * FROM t'
func getPlanFromSimpleExplain(rows *sql.Rows) (*ExecutionPlan, error) {
	var id, task, accessObject, operatorInfo string
	var estRows float64

	if err := rows.Scan(&id, &estRows, &task, &accessObject, &operatorInfo); err != nil {
		return nil, fmt.Errorf("failed to scan standard execution plan line: %w", err)
	}
	//fmt.Printf("\nExplain5 id:\n%s\n", id)

	var retPlan, currPlan *ExecutionPlan
	for {
		plan := &ExecutionPlan{
			ID:           id,
			Task:         task,
			OperatorInfo: operatorInfo,
			EstRows:      estRows,
			AccessObject: accessObject,
		}
		if retPlan == nil {
			retPlan = plan
			currPlan = plan
		} else {
			currPlan.Next = plan
			currPlan = plan
		}
		if !rows.Next() {
			break
		}
	}
	return retPlan, nil
}

// EXPLAIN ANALYZE format: id, estRows, actRows, task, access object, execution info, operator info, memory, disk
func getPlanFromExplainAnalyze(rows *sql.Rows) (*ExecutionPlan, error) {
	var id, task, accessObject, executionInfo, operatorInfo, memory, disk string
	var estRows, actRows float64

	var retPlan, currPlan *ExecutionPlan
	for {
		if err := rows.Scan(&id, &estRows, &actRows, &task, &accessObject, &executionInfo, &operatorInfo, &memory, &disk); err != nil {
			return nil, fmt.Errorf("failed to scan analyze execution plan line: %w", err)
		}
		//fmt.Printf("\nExplain9 id: %s\t%s\t%s\n", id, accessObject, executionInfo)

		plan := &ExecutionPlan{
			ID:            id,
			Task:          task,
			EstRows:       estRows,
			ActRows:       int64(actRows),
			OperatorInfo:  operatorInfo,
			ExecutionInfo: executionInfo,
			Memory:        memory,
			Disk:          disk,
			AccessObject:  accessObject,
		}
		if retPlan == nil {
			retPlan = plan
			currPlan = plan
		} else {
			currPlan.Next = plan
			currPlan = plan
		}
		if !rows.Next() {
			break
		}
	}
	return retPlan, nil
}

// Standard EXPLAIN format: id, estRows, task, access object, operator info

// parseTabularExecutionPlan parses a tabular format execution plan
func (c *TiDBClient) parseTabularExecutionPlan(rows *sql.Rows) (*ExecutionPlan, error) {
	if !rows.Next() {
		return nil, fmt.Errorf("no execution plan found")
	}

	// Get column information to determine the format
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column information: %w", err)
	}

	// Handle different EXPLAIN formats based on number of columns
	if len(columns) == 5 {
		return getPlanFromSimpleExplain(rows)
	} else if len(columns) == 9 {
		return getPlanFromExplainAnalyze(rows)
	} else {
		return nil, fmt.Errorf("unsupported EXPLAIN format with %d columns: %v", len(columns), columns)
	}
}

// Close closes the database connection
func (c *TiDBClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ExecuteQueryWithMetrics executes a query and captures performance metrics
func (c *TiDBClient) ExecuteQueryWithMetrics(testScenario TestScenario) (*TestExecutionResult, error) {
	return c.executeQueryWithMetrics(testScenario, true)
}

// ExecuteQueryWithMetrics executes a query and captures performance metrics
func (c *TiDBClient) executeQueryWithMetrics(testScenario TestScenario, retry bool) (*TestExecutionResult, error) {
	res := &TestExecutionResult{
		ScenarioID:  testScenario.ID,
		Variant:     testScenario.Variant,
		Query:       testScenario.Query,
		ExplainOnly: testScenario.ExplainOnly,
	}
	query := testScenario.Query

	if testScenario.ExplainOnly {
		// Get execution plan first
		plan, err := c.GetExecutionPlan(query)
		if err != nil {
			return nil, err
		}
		// Analyze the execution plan to determine plan type
		res.PlanType = determinePlanType(plan)
		return res, nil
	}
	startTime := time.Now()
	id, _ := c.getConnectionID()
	slog.Debug("Executing query", "connection id", id, "conid", c.connectionID)
	c.connectionID = id

	// Execute the query and count rows
	rows, err := c.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Count returned rows
	var rowCount int64
	var aVal, bVal int
	var cVal string
	for rows.Next() {
		if rowCount == 0 {
			if err := rows.Scan(&aVal, &bVal, &cVal); err != nil {
				return nil, err
			}
		}
		rowCount++
	}

	res.ExecutionTime = time.Since(startTime)

	plan, err := c.GetActualExecutionPlan()
	if err != nil {
		return nil, err
	}

	res.PlanType = determinePlanType(plan)

	if isCoprCacheUsed(plan) {
		if !retry {
			return nil, fmt.Errorf("execution coprocessor cache is used")
		}
		b := strconv.Itoa(bVal)
		slog.Info("coprocessor cache used, updating values", "value", b, "rowCount", rowCount, "a", aVal, "b", bVal, "c", cVal)
		// cache is used, try to update all b values and then back again, to invalidate the cache
		var count int
		for {
			_, err = c.ExecuteQuery("UPDATE " + testScenario.TableName + " SET b = -313 where b = " + b + " LIMIT 50000")
			if err != nil {
				return nil, err
			}
			err = c.db.QueryRow("SELECT COUNT(*) FROM " + testScenario.TableName + " WHERE b = " + b).Scan(&count)
			if err != nil {
				return nil, fmt.Errorf("failed to get count: %w", err)
			}
			if count == 0 {
				break
			}
		}
		for {
			_, err = c.ExecuteQuery("UPDATE " + testScenario.TableName + " SET b = " + b + " where b = -313 LIMIT 50000")
			if err != nil {
				return nil, err
			}
			err = c.db.QueryRow("SELECT COUNT(*) FROM " + testScenario.TableName + " WHERE b = -313").Scan(&count)
			if err != nil {
				return nil, fmt.Errorf("failed to get count: %w", err)
			}
			if count == 0 {
				break
			}
		}
		return c.executeQueryWithMetrics(testScenario, false)
	}
	res.RowsReturned = rowCount
	return res, nil
}

// determinePlanType analyzes the execution plan to determine if it's index lookup or table scan
func determinePlanType(plan *ExecutionPlan) string {
	if plan == nil {
		return "unknown"
	}

	// Check the root operator
	id := strings.ToLower(plan.ID)
	if strings.Contains(id, "index") && !strings.Contains(id, "table") {
		return "index_lookup"
	} else if strings.Contains(id, "tablereader") {
		return "table_scan"
	}

	// Check children for more specific information
	for plan = plan.Next; plan != nil; plan = plan.Next {
		planType := determinePlanType(plan)
		if planType != "unknown" {
			return planType
		}
	}

	return "unknown"
}

func isCoprCacheUsed(plan *ExecutionPlan) bool {
	if plan == nil {
		return false
	}

	r, err := regexp.Compile(`copr_cache_hit_ratio: ([01]\.[0-9][0-9])`)
	if err != nil {
		return false
	}
	matches := r.FindAllStringSubmatch(plan.ExecutionInfo, -1)
	for _, match := range matches {
		if len(match) == 2 && match[1] != "0.00" {
			slog.Info("copr_cache_hit_ratio match", "match", match)
			return true
		}
	}
	return isCoprCacheUsed(plan.Next)
}

// getConnectionID returns the current connection ID
func (c *TiDBClient) getConnectionID() (int, error) {
	if c.db == nil {
		return 0, fmt.Errorf("database connection not established")
	}

	var connectionID int
	slog.Debug("Executing query", "query", "SELECT CONNECTION_ID()")
	err := c.db.QueryRow("SELECT CONNECTION_ID()").Scan(&connectionID)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection ID: %w", err)
	}

	return connectionID, nil
}

// GetActualExecutionPlan returns the actual execution plan for a connection
func (c *TiDBClient) GetActualExecutionPlan() (*ExecutionPlan, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Use EXPLAIN FOR CONNECTION to get the actual plan in tabular format
	explainQuery := fmt.Sprintf("EXPLAIN FOR CONNECTION %d", c.connectionID)
	slog.Debug("Executing query", "query", explainQuery)
	rows, err := c.db.Query(explainQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual execution plan: %w", err)
	}
	defer rows.Close()

	// Parse the tabular format execution plan
	return c.parseTabularExecutionPlan(rows)
}
