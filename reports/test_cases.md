# Test Cases: Flight Price Analysis ETL Pipeline

## 1. Introduction

This document outlines the test cases used to validate the correctness, reliability, and robustness of the Flight Price Analysis ETL pipeline.

The testing strategy ensures:

* Data integrity across all pipeline stages
* Correct KPI computation
* Proper error handling
* End-to-end pipeline reliability

---

## 2. Testing Approach

The pipeline was tested at three levels:

1. **Unit-Level Testing** – Individual task validation
2. **Integration Testing** – Data flow between tasks
3. **End-to-End Testing** – Full DAG execution

---

## 3. Test Cases

### Test Case 1: CSV Ingestion Success

**Objective:**
Verify that data is successfully loaded from CSV into MySQL.

**Input:**
Valid CSV file

**Steps:**

1. Trigger DAG
2. Run `load_csv_to_mysql` task

**Expected Result:**

* Data loaded into MySQL staging table
* Log shows total loaded rows
* No errors

**Validation Query:**

```sql
SELECT COUNT(*) FROM flight_price_staging;
```

---

###  Test Case 2: Missing Required Columns

**Objective:**
Ensure pipeline fails when required columns are missing.

**Input:**
CSV file missing `airline` or `source`

**Steps:**

1. Modify CSV to remove required column
2. Trigger DAG

**Expected Result:**

* Task fails
* Error message: "Missing required columns"

---

### Test Case 3: Empty CSV File

**Objective:**
Ensure pipeline fails when no valid data is present.

**Input:**
Empty CSV or all invalid rows

**Expected Result:**

* Pipeline raises error
* Message: "No valid rows were loaded"

---

### Test Case 4: Data Validation (Null Handling)

**Objective:**
Ensure invalid rows are removed during validation.

**Input:**
CSV with null values

**Expected Result:**

* Rows with null/empty critical fields are removed
* Validated count ≤ loaded count

---

### Test Case 5: Negative Values Handling

**Objective:**
Ensure negative fares are corrected.

**Input:**
CSV with negative `base_fare` or `tax`

**Expected Result:**

* Values converted to positive
* No negative values remain

---

### Test Case 6: Row Count Mismatch

**Objective:**
Ensure pipeline detects inconsistencies between stages.

**Steps:**

1. Manually modify staging table
2. Run transformation task

**Expected Result:**

* Pipeline fails
* Error: "Row count mismatch"

---

### Test Case 7: KPI Computation Accuracy

**Objective:**
Verify correctness of KPI calculations.

**Expected Results:**

* Average fare correctly computed
* Booking counts accurate
* Popular routes sorted correctly

---

### Test Case 8: Data Lineage (run_id)

**Objective:**
Ensure historical data is preserved.

**Steps:**

1. Run DAG multiple times

**Validation Query:**

```sql
SELECT run_id, COUNT(*) 
FROM transformed_flight_prices
GROUP BY run_id;
```

**Expected Result:**

* Multiple `run_id` values
* No data overwritten

---

### Test Case 9: Database Connection Failure

**Objective:**
Ensure pipeline handles connection issues.

**Steps:**

1. Stop MySQL or PostgreSQL container
2. Trigger DAG

**Expected Result:**

* Task fails
* Error logged
* No partial data committed

---

### Test Case 10: Full Pipeline Execution

**Objective:**
Verify end-to-end pipeline functionality.

**Steps:**

1. Trigger DAG

**Expected Result:**

* All tasks succeed
* Data flows correctly through all stages
* KPIs stored in PostgreSQL

---

## Summary

The pipeline successfully passes all critical test scenarios, ensuring:

* Data correctness
* Robust error handling
* Reliable orchestration
* Historical data tracking

---

## Conclusion

The implemented test cases confirm that the ETL pipeline is:

✔ Reliable
✔ Accurate
✔ Production-ready

---
