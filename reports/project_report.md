# Project Report: Flight Price Analysis ETL Pipeline

## 1. Introduction

This project implements an end-to-end **ETL (Extract, Transform, Load) pipeline** using **Apache Airflow 3.2.1** to process and analyze flight price data.

The objective is to design a robust, automated data pipeline that:

* Ingests raw data from a CSV file
* Cleans and validates the data
* Computes key performance indicators (KPIs)
* Stores results in an analytics database

The pipeline follows **data engineering best practices**, ensuring scalability, reliability, and maintainability.

---

## 2. Problem Statement

The initial pipeline had several critical issues:

* Fragmented DAG architecture (multiple DAGs)
* No orchestration between stages
* Lack of error handling and logging
* Inefficient data loading (no batching)
* No data validation between stages
* No history tracking (data overwritten each run)

These issues violated core Airflow principles and limited the pipeline’s reliability.

---

## 3. Solution Overview

The pipeline was redesigned into a **single, fully orchestrated DAG**, ensuring:

* End-to-end workflow automation
* Proper task dependencies
* Robust validation and monitoring
* Historical data tracking using `run_id`

---

## 4. Architecture

### Data Flow

```text
CSV → MySQL (Staging) → Validation → Transformation → PostgreSQL (Analytics)
```

### DAG Structure

```text
load_csv_to_mysql → validate_data_mysql → transform_and_compute_kpis
```

Each stage is implemented as a separate Airflow task and connected through dependencies.

---

## 5. Implementation Details

### 5.1 Data Ingestion (Extract & Load)

* Data is read from CSV using **Pandas**
* Implemented **chunking (batch processing)** to handle large datasets efficiently
* Column names are cleaned and standardized
* Data is loaded into a MySQL staging table

**Key Features:**

* Chunk size: 1000 rows
* Logging for each chunk processed
* Error handling with rollback
* Returns row count via XCom

---

### 5.2 Data Validation

Validation is performed in the MySQL staging layer:

* Removes rows with null or empty critical fields
* Corrects negative values
* Ensures numeric consistency
* Verifies row count integrity

**Key Validation Rule:**

* Validated row count must be ≤ loaded row count

---

### 5.3 Data Transformation & KPI Computation

Transformation is performed using **Pandas**:

#### KPIs Computed:

* Average fare by airline
* Booking count by airline
* Popular routes
* Seasonal fare variation (placeholder)

**Data Lineage:**

* Each run is assigned a unique `run_id`
* Results are appended to PostgreSQL tables
* Historical data is preserved

---

### 5.4 Orchestration (Airflow)

The pipeline uses a **single DAG** to ensure proper orchestration:

* Tasks communicate using **XCom**
* Daily scheduling is configured
* Retries and timeouts are implemented
* Full pipeline automation is achieved

---

## Error Handling & Logging

The pipeline includes:

* Try/except blocks in all stages
* Database rollback on failure
* Detailed logging at each step
* Clear error messages for debugging

---

## Configuration Management

To improve flexibility:

* Hardcoded values were removed
* Airflow Variables are used:

  * `mysql_conn_id`
  * `postgres_conn_id`
  * `csv_path`

This allows dynamic configuration without modifying code.

---

## Testing Strategy

Testing was performed to ensure pipeline correctness:

* Row count validation between stages
* Data integrity checks
* End-to-end DAG execution testing
* Error scenario handling

Detailed test cases are documented in `test_cases.md`.

---

## Results

The pipeline successfully:

* Processes ~57,000 records per run
* Executes in under 1 minute
* Maintains historical data across runs
* Provides accurate KPI outputs

---

## Limitations

* Seasonal KPI is a placeholder due to missing date column
* No deduplication strategy for repeated runs
* Schema names are partially hardcoded

---

## Future Improvements

* Integrate **dbt** for SQL-based transformations
* Add time-based seasonal analysis
* Implement incremental loading strategies
* Add monitoring dashboards

---

## Conclusion

This project demonstrates the design and implementation of a **robust ETL pipeline** using Airflow.

All major issues from the initial implementation were resolved, resulting in a **fully orchestrated, reliable, and production-ready pipeline**.

---

## Author

**Denyse AGAHOZO**

