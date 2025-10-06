# Quick Start Guide

## Setup

1. Ensure you have a TiDB cluster running (e.g., on `127.0.0.1:4000`)

2. Build the tool:
   ```bash
   make build
   # or
   go build -o tidb-optimizer-calibration .
   ```

## Step 1: Create Test Table

Create the test table with sample data:

```bash
./tidb-optimizer-calibration -create-table -dsn "root@tcp(127.0.0.1:4000)/test"
# or using make
make create-table DSN="root@tcp(127.0.0.1:4000)/test"
```

This will:
- Drop table `t` if it exists
- Create table with schema: `t(a bigint auto_increment primary key, b int, c varchar(255), key(b), key(c,b))`
- Insert 10,000 rows of test data
- Run `ANALYZE TABLE` to update statistics

## Step 2: Run Optimizer Tests

Test the optimizer decisions:

```bash
./tidb-optimizer-calibration -test-optimizer -dsn "root@tcp(127.0.0.1:4000)/test"
# or using make
make test-optimizer DSN="root@tcp(127.0.0.1:4000)/test"
```

This will run 9 different test scenarios and show:
- The SQL query
- Expected optimizer behavior
- Actual execution plan (EXPLAIN output)
- Execution analysis with timing (EXPLAIN ANALYZE output)

## Test Scenarios Covered

1. **Full Table Scan**: `SELECT * FROM t`
   - Tests when TableFullScan is chosen

2. **Primary Key Point Query**: `SELECT * FROM t WHERE a = 100`
   - Should use Point_Get

3. **Primary Key Range Query**: `SELECT * FROM t WHERE a >= 100 AND a <= 200`
   - Should use TableRangeScan

4. **Index Lookup (b)**: `SELECT * FROM t WHERE b = 10`
   - Should use IndexLookUp on idx_b

5. **Covering Index (b)**: `SELECT a, b FROM t WHERE b = 10`
   - Should use IndexReader on idx_b

6. **Composite Index Query**: `SELECT * FROM t WHERE c = 'value_10'`
   - Should use IndexLookUp on idx_cb

7. **Composite Index Covering**: `SELECT c, b FROM t WHERE c = 'value_10'`
   - Should use IndexReader on idx_cb

8. **Non-selective Query**: `SELECT * FROM t WHERE b > 50`
   - May use TableFullScan due to low selectivity

9. **Highly Selective Query**: `SELECT * FROM t WHERE b = 1`
   - Should use IndexLookUp due to high selectivity

## Understanding the Output

### Execution Plan (EXPLAIN)
Shows the planned execution strategy, including:
- Operator types (Point_Get, TableFullScan, IndexLookUp, etc.)
- Estimated rows
- Task type (root, cop)
- Access object (table/index used)

### Execution Analysis (EXPLAIN ANALYZE)
Shows actual execution metrics:
- Actual execution time
- Actual rows processed
- Memory usage
- Comparison of estimated vs actual

## Customization

You can modify `main.go` to add your own test queries. Each test entry includes:
- `name`: Descriptive name
- `query`: SQL query to test
- `description`: Expected optimizer behavior

Example:
```go
{
    name:        "My Custom Test",
    query:       "SELECT b, COUNT(*) FROM t GROUP BY b",
    description: "Expected behavior description",
},
```
