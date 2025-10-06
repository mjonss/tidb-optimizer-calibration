# tidb-optimizer-calibration

TiDB tests for validating and calibrating the optimizer costs

## Overview

This tool tests the TiDB optimizer using real data and queries to validate if the choices made are reasonable. It specifically tests scenarios like table scan vs index lookup decisions.

## Prerequisites

- A running TiDB cluster (this tool assumes TiDB is already set up)
- Go 1.16 or later

## Building

```bash
go build -o tidb-optimizer-calibration .
```

## Usage

The tool has two main modes of operation:

### 1. Create Test Table

First, create a test table with data:

```bash
./tidb-optimizer-calibration -create-table -dsn "root@tcp(127.0.0.1:4000)/test"
```

This creates a table with the following schema:
```sql
CREATE TABLE t (
    a BIGINT AUTO_INCREMENT PRIMARY KEY,
    b INT,
    c VARCHAR(255),
    KEY idx_b (b),
    KEY idx_cb (c, b)
)
```

The table is populated with 10,000 rows of test data.

### 2. Run Optimizer Tests

After creating the table, run the optimizer tests:

```bash
./tidb-optimizer-calibration -test-optimizer -dsn "root@tcp(127.0.0.1:4000)/test"
```

This runs a series of queries and displays:
- The query being tested
- Expected optimizer behavior
- Actual execution plan (EXPLAIN output)
- Execution analysis (EXPLAIN ANALYZE output)

## Test Scenarios

The tool tests various optimizer decision scenarios:

1. **Full table scan** - When all columns are needed
2. **Primary key point query** - Point_Get on primary key
3. **Primary key range query** - TableRangeScan on primary key
4. **Index lookup** - IndexLookUp when table lookup is needed
5. **Covering index** - IndexReader when index covers all needed columns
6. **Composite index queries** - Testing multi-column indexes
7. **Selectivity tests** - High vs low selectivity queries

## Configuration

The DSN (Data Source Name) format is:

```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

Examples:
- `root@tcp(127.0.0.1:4000)/test` - Default local TiDB
- `user:password@tcp(192.168.1.100:4000)/test` - Remote TiDB with authentication
- `root@tcp(localhost:4000)/mydb?parseTime=true` - With additional parameters

## Example Output

Each test shows:
```
--- Test 1: Primary key point query ---
Query: SELECT * FROM t WHERE a = 100
Description: Should use Point_Get on primary key

Execution Plan:
id      estRows task    access object   operator info
Point_Get_1     1.00    root    table:t handle:100

Execution Analysis:
...
```

## License

Apache License 2.0
