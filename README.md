#  Flight Price Analysis ETL Pipeline

##  Overview

This project implements an end-to-end **ETL pipeline** using **Apache Airflow 3.2.1** to process and analyze flight price data.

The pipeline:

* Extracts data from a CSV file
* Loads it into a MySQL staging database
* Validates and cleans the data
* Computes key performance indicators (KPIs)
* Stores results in PostgreSQL for analytics

It follows **production-level data engineering practices**, including orchestration, validation, batching, logging, and data lineage tracking.

---

## Architecture

```text
CSV File
   ↓
MySQL (Staging)
   ↓
Validation
   ↓
Transformation & KPI Computation
   ↓
PostgreSQL (Analytics)
```

---

##  Workflow (Airflow DAG)

```text
load_csv_to_mysql → validate_data_mysql → transform_and_compute_kpis
```

* Fully orchestrated in a **single DAG**
* Tasks communicate using **XCom**
* Pipeline runs automatically on a **daily schedule**

---

##  Technologies Used

* **Apache Airflow 3.2.1** – Workflow orchestration
* **Python (Pandas)** – Data processing
* **MySQL** – Staging database
* **PostgreSQL** – Analytics database
* **Docker** – Containerized environment

---

##  Key Features

###  Data Ingestion

* Chunked CSV loading (batch processing)
* Handles large datasets efficiently
* Dynamic column cleaning

### Data Validation

* Removes invalid and null records
* Corrects negative values
* Ensures data consistency

###  KPI Computation

* Average fare by airline
* Booking count by airline
* Popular routes
* Seasonal fare placeholder

###  Orchestration

* Single DAG (fixes fragmentation issue)
* Task dependencies managed properly
* Retry and timeout configurations

###  Data Lineage

* `run_id` added to all PostgreSQL tables
* Preserves historical runs
* Prevents data loss

###  Logging & Error Handling

* Detailed logs for all tasks
* Exception handling with rollback
* Clear failure messages

###  Configuration Management

* Uses **Airflow Variables** instead of hardcoding:

  * `mysql_conn_id`
  * `postgres_conn_id`
  * `csv_path`

---

##  Project Structure

```text
flight_price_analysis_project/
│
├── dags/
│   └── flight_price_analysis_dag.py
│
├── src/
│   ├── extract_load.py
│   ├── validate.py
│   ├── transform_kpis.py
│   └── __init__.py
│
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
│
├── reports/
│   ├── project_report.md
│   ├── test_cases.md
│   └── user_guide.md
│
├── docker-compose.yml
└── README.md
```

---

##  Documentation

Additional documentation is available in the `reports/` folder:

* `project_report.md` – Detailed pipeline design and implementation
* `test_cases.md` – Validation and testing scenarios
* `user_guide.md` – Instructions for setup and execution

---

## How to Run

### 1. Start Services

```bash
docker compose up -d
```

---

### 2. Open Airflow

```text
http://localhost:8081
```

Login:

```
username: admin
password: admin
```

---

### 3. Configure

* Add **Connections** (MySQL & PostgreSQL)
* Add **Variables**:

  ```
  mysql_conn_id = mysql_raw
  postgres_conn_id = postgres_analytics
  csv_path = /opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv
  ```

---

### 4. Run Pipeline

* Open DAG: `flight_price_analysis_v5`
* Click **Trigger DAG**
* Monitor in Graph View

---

##  Verification

### Check Data in PostgreSQL

```sql
SELECT run_id, COUNT(*) 
FROM transformed_flight_prices
GROUP BY run_id;
```

✔ Confirms historical tracking
✔ Ensures no data loss

---

##  Limitations

* Seasonal KPI is a placeholder (no date column available)
* No deduplication strategy for repeated runs
* Further parameterization possible

---

## Future Improvements

* Integrate **dbt** for transformation layer
* Add time-based seasonal analysis
* Implement incremental loading strategies
* Build monitoring dashboards

---

## Achievements

* Fully orchestrated ETL pipeline
* Efficient batch processing
* Robust validation and error handling
* Data lineage with historical tracking
* Production-ready architecture

---

## Author

**Denyse AGAHOZO**
---

