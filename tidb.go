package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
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
	ID           string                 `json:"id"`
	Task         string                 `json:"task"`
	OperatorInfo string                 `json:"operator info"`
	Count        int64                  `json:"count"`
	EstRows      float64                `json:"estRows"`
	EstCost      float64                `json:"estCost"`
	ActRows      int64                  `json:"actRows"`
	ActTime      string                 `json:"actTime"`
	Memory       string                 `json:"memory"`
	Disk         string                 `json:"disk"`
	AccessObject string                 `json:"access object"`
	Children     []*ExecutionPlan       `json:"children,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
}

// ActualExecutionPlan represents the actual execution plan from TiDB (different format)
type ActualExecutionPlan struct {
	ID           interface{}            `json:"id"`
	Task         string                 `json:"task"`
	OperatorInfo string                 `json:"operator info"`
	Count        interface{}            `json:"count"`
	EstRows      interface{}            `json:"estRows"`
	EstCost      interface{}            `json:"estCost"`
	ActRows      interface{}            `json:"actRows"`
	ActTime      string                 `json:"actTime"`
	Memory       string                 `json:"memory"`
	Disk         string                 `json:"disk"`
	AccessObject string                 `json:"access object"`
	Children     []*ActualExecutionPlan `json:"children,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
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

	// Try TiDB JSON format first, fall back to text format
	explainQuery := fmt.Sprintf("EXPLAIN FORMAT=\"brief\" %s", query)
	//explainQuery := fmt.Sprintf("EXPLAIN FORMAT=\"tidb_json\" %s", query)
	var explainBrief string
	slog.Debug("Executing query", "query", explainQuery)
	err := c.db.QueryRow(explainQuery).Scan(&explainBrief)
	if err != nil {
		return nil, err
	}

	// Parse the TiDB JSON execution plan (returns an array)
	var plans []ActualExecutionPlan
	if err := json.Unmarshal([]byte(explainBrief), &plans); err != nil {
		return nil, fmt.Errorf("failed to parse actual execution plan JSON: %w", err)
	}

	if len(plans) == 0 {
		return nil, fmt.Errorf("no actual execution plan found in JSON")
	}

	// Convert ActualExecutionPlan to ExecutionPlan
	plan := c.convertActualToExecutionPlan(&plans[0])
	return plan, nil
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
	res := &TestExecutionResult{
		ScenarioID:  testScenario.ID,
		Variant:     testScenario.Variant,
		Query:       testScenario.Query,
		ExplainOnly: testScenario.ExplainOnly,
	}
	query := testScenario.Query

	startTime := time.Now()
	if testScenario.ExplainOnly {
		// Get execution plan first
		plan, err := c.GetExecutionPlan(query)
		if err != nil {
			return nil, err
		}
		// Analyze the execution plan to determine plan type
		res.PlanType = c.determinePlanType(plan)
		res.PlanDetails = c.getPlanDetails(plan)
		return res, nil
	}
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
	for rows.Next() {
		rowCount++
	}

	res.ExecutionTime = time.Since(startTime)

	plan, err := c.GetActualExecutionPlan()
	if err != nil {
		return nil, err
	}

	res.PlanType = c.determinePlanType(plan)
	res.PlanDetails = c.getPlanDetails(plan)
	res.RowsReturned = rowCount
	return res, nil
}

// determinePlanType analyzes the execution plan to determine if it's index lookup or table scan
func (c *TiDBClient) determinePlanType(plan *ExecutionPlan) string {
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
	for _, child := range plan.Children {
		childType := c.determinePlanType(child)
		if childType != "unknown" {
			return childType
		}
	}

	return "unknown"
}

// getPlanDetails returns a summary of the execution plan
func (c *TiDBClient) getPlanDetails(plan *ExecutionPlan) string {
	if plan == nil {
		return "No plan available"
	}

	var details []string
	details = append(details, fmt.Sprintf("Root: %s", plan.OperatorInfo))
	details = append(details, fmt.Sprintf("EstRows: %.2f", plan.EstRows))
	details = append(details, fmt.Sprintf("EstCost: %.2f", plan.EstCost))

	if plan.AccessObject != "" {
		details = append(details, fmt.Sprintf("Access: %s", plan.AccessObject))
	}

	return strings.Join(details, ", ")
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

	// Use EXPLAIN FOR CONNECTION to get the actual plan
	explainQuery := fmt.Sprintf("EXPLAIN FORMAT=\"brief\" FOR CONNECTION %d", c.connectionID)
	//explainQuery := fmt.Sprintf("EXPLAIN FORMAT=\"tidb_json\" FOR CONNECTION %d", c.connectionID)
	var explainBrief string
	slog.Debug("Executing query", "query", explainQuery)
	err := c.db.QueryRow(explainQuery).Scan(&explainBrief)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual execution plan: %w", err)
	}
	// Enable for simpler debugging...
	fmt.Printf("\nEXPLAIN:\n%s\n\n", explainBrief)

	// Parse the TiDB JSON execution plan (returns an array)
	var actualPlans []ActualExecutionPlan
	if err := json.Unmarshal([]byte(explainBrief), &actualPlans); err != nil {
		return nil, fmt.Errorf("failed to parse actual execution plan JSON: %w", err)
	}

	if len(actualPlans) == 0 {
		return nil, fmt.Errorf("no actual execution plan found in JSON")
	}

	// Convert ActualExecutionPlan to ExecutionPlan
	plan := c.convertActualToExecutionPlan(&actualPlans[0])
	return plan, nil
}

// convertActualToExecutionPlan converts ActualExecutionPlan to ExecutionPlan
func (c *TiDBClient) convertActualToExecutionPlan(actual *ActualExecutionPlan) *ExecutionPlan {
	plan := &ExecutionPlan{
		Task:         actual.Task,
		OperatorInfo: actual.OperatorInfo,
		ActTime:      actual.ActTime,
		Memory:       actual.Memory,
		Disk:         actual.Disk,
		AccessObject: actual.AccessObject,
		Details:      actual.Details,
	}

	// Convert ID from interface{} to int
	if actual.ID != nil {
		switch v := actual.ID.(type) {
		case string:
			plan.ID = v
		case float64:
			plan.ID = strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			plan.ID = strconv.Itoa(v)
		}
	}

	// Convert Count from interface{} to int64
	if actual.Count != nil {
		switch v := actual.Count.(type) {
		case string:
			if count, err := strconv.ParseInt(v, 10, 64); err == nil {
				plan.Count = count
			}
		case float64:
			plan.Count = int64(v)
		case int64:
			plan.Count = v
		}
	}

	// Convert EstRows from interface{} to float64
	if actual.EstRows != nil {
		switch v := actual.EstRows.(type) {
		case string:
			if rows, err := strconv.ParseFloat(v, 64); err == nil {
				plan.EstRows = rows
			}
		case float64:
			plan.EstRows = v
		}
	}

	// Convert EstCost from interface{} to float64
	if actual.EstCost != nil {
		switch v := actual.EstCost.(type) {
		case string:
			if cost, err := strconv.ParseFloat(v, 64); err == nil {
				plan.EstCost = cost
			}
		case float64:
			plan.EstCost = v
		}
	}

	// Convert ActRows from interface{} to int64
	if actual.ActRows != nil {
		switch v := actual.ActRows.(type) {
		case string:
			if rows, err := strconv.ParseInt(v, 10, 64); err == nil {
				plan.ActRows = rows
			}
		case float64:
			plan.ActRows = int64(v)
		case int64:
			plan.ActRows = v
		}
	}

	// Convert children recursively
	if len(actual.Children) > 0 {
		plan.Children = make([]*ExecutionPlan, len(actual.Children))
		for i, child := range actual.Children {
			plan.Children[i] = c.convertActualToExecutionPlan(child)
		}
	}

	return plan
}
