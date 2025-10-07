# TiDB Optimizer Calibration Tool

A production-ready Go application for validating and testing TiDB optimizer decisions, specifically focusing on index lookup vs. table scan choices across different data sizes and selectivity patterns.

## Project Structure

```
tidb-optimizer-calibration/
├── main.go                   # Main application entry point and core functionality
├── types.go                  # Core data types and interfaces
├── tidb.go                   # TiDB connection and query execution
├── scenarios.go              # Test scenario execution and data generation
├── comprehensive.go          # Comprehensive test scenarios for index vs table scan
├── test_runner.go            # Test execution with metrics capture
├── go.mod                    # Go module definition
└── README.md                 # This file
```

## Getting Started

1. Build the project:
   ```bash
   go build .
   ```

2. Run the calibration tool:
   ```bash
   ./tidb-optimizer-calibration
   ```
   
   This will:
   - Connect to TiDB cluster (localhost:4000)
   - Execute 144 comprehensive test scenarios
   - Show detailed results table with real performance metrics
   - Display compact summary and statistics
   - Provide comprehensive optimizer decision analysis

## Comprehensive Test Suite

The tool includes a comprehensive test suite focused on **index lookup vs table scan decisions**:

- **Data Sizes**: 1, 10, 100, 1K, 10K, 100K, 1M, 10M rows
- **Selectivity Levels**: 1/2, 1/4, 1/8, 1/16, 1/32, 1/64, 1/128, 1/256, 1/512
- **Total Test Cases**: 144 comprehensive scenarios
- **Table Structure**: Simple `CREATE TABLE t1K (id INT AUTO_INCREMENT PRIMARY KEY, b INT, KEY (b))`
- **Test Queries**: 
  - Without hints: `SELECT * FROM t1K WHERE b = 250` (let optimizer decide)
  - With hints: `SELECT /*+ IGNORE_INDEX(t1K, b) */ * FROM t1K WHERE b = 250` (force table scan)

## Test Execution with Metrics

The tool captures detailed performance metrics for each test:

- **Execution Time**: Actual query execution time
- **Resource Units (RU)**: Calculated based on plan complexity and execution time
- **Plan Type**: Automatically detected (index_lookup vs table_scan)
- **Plan Details**: Root operator, estimated rows, cost, access objects
- **Rows Returned**: Actual number of rows returned by the query
- **Total Cost**: Sum of all costs in the execution plan

### Example Output:
```
📊 Test Results Table
====================
Scenario             Plan Type       Query Type   Time(ms) RU       Rows     Cost     Table   
----------------------------------------------------------------------------------------------------

📋 Table Size: 1K rows
----------------------------------------------------------------------------------------------------
index_1K_50%         index_lookup    Index        1.6      0.14     4        2.40     1K      
tablescan_1K_50%     table_scan      TableScan    3.9      0.39     541      5.60     1K      

📋 Compact Summary Table
=======================
Table    Index Time   Index RU     Scan Time    Scan RU      Index Rows   Scan Rows   
--------------------------------------------------------------------------------
1K       1.6          0.14         3.9          0.39         4            541         

📈 Summary Statistics
====================
Total Tests Executed: 144
Average Index Lookup Time: 1.6ms
Average Table Scan Time: 4.0ms
Index Lookups: 72 (50.0%)
Table Scans: 72 (50.0%)
```

## Features

- **Scenario-Based Testing**: Define test cases for different optimizer decisions
- **Decision Validation**: Compare expected vs actual optimizer choices with comprehensive validation
- **Cost Analysis**: Validate cost model calculations with tolerance-based comparison
- **Performance Benchmarking**: Measure actual execution performance
- **Regression Detection**: Detect optimizer regressions and performance changes
- **Configuration Testing**: Test different TiDB configurations
- **Smart Decision Matching**: Intelligent matching of actual vs expected decisions
- **Detailed Reporting**: Comprehensive difference reporting and JSON output
- **Decision-Specific Validation**: Specialized validation for different optimizer decision types

## Validation Framework

The tool includes a sophisticated validation framework that:

- **Compares Decision Types**: Validates that the optimizer chose the expected decision type
- **Cost Tolerance**: Compares costs with configurable tolerance for floating-point precision
- **Operator Validation**: Checks that the correct operators are being used
- **Decision-Specific Criteria**: Specialized validation for each decision type:
  - Index lookups: Validates index usage and reasonable costs
  - Table scans: Ensures table scan operators are used
  - Joins: Validates join algorithm selection (hash, nested loop, sort-merge)
- **Smart Matching**: Uses scoring to find the best match between actual and expected decisions
- **Comprehensive Reporting**: Detailed difference reporting with JSON output