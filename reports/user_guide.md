# User Guide: Flight Price Analysis ETL Pipeline

## 1. Introduction

This guide provides step-by-step instructions for setting up, running, and monitoring the Flight Price Analysis ETL pipeline.

The pipeline uses **Apache Airflow**, **MySQL**, **PostgreSQL**, and **dbt**, all running in a **Docker environment**.

---

## 2. Prerequisites

Ensure the following are installed:

* Docker & Docker Compose
* Git
* VS Code (or any code editor)

---

## 3. Project Setup

### Step 1: Clone the Repository

```bash
git clone <your-repository-url>
cd flight_price_analysis_project
```

---

### Step 2: Start Services

Run the following command:

```bash
docker compose up -d --build
```

This will start:

* Airflow services (scheduler, apiserver, triggerer, dag-processor)
* MySQL (staging database)
* PostgreSQL (analytics database)

---

### Step 3: Verify Containers

```bash
docker ps
```

Ensure all containers show:

```text
healthy
```

---

## 4. Airflow Setup

### Step 1: Open Airflow UI

```text
http://localhost:8081
```

### Step 2: Login

```text
Username: admin
Password: admin
```

---

### Step 3: Configure Connections

Go to:

```text
Admin → Connections
```

Add:

#### MySQL Connection

* Connection ID: `mysql_raw`
* Host: `mysql`
* Schema: `airflow`
* Login: `airflow`
* Password: `airflow`
* Port: `3306`

#### PostgreSQL Connection

* Connection ID: `postgres_analytics`
* Host: `postgres`
* Schema: `airflow`
* Login: `airflow`
* Password: `airflow`
* Port: `5432`

---

### Step 4: Configure Variables

Go to:

```text
Admin → Variables
```

Add:

```text
mysql_conn_id = mysql_raw
postgres_conn_id = postgres_analytics
csv_path = /opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv
```

---

## 5. Running the Pipeline

### Step 1: Locate DAG

Go to:

```text
DAGs → flight_price_analysis_v5
```

---

### Step 2: Trigger DAG

Click:

```text
Trigger DAG
```

---

### Step 3: Monitor Execution

Go to:

```text
Graph View
```

Expected:

```text
load_csv_to_mysql → validate_data_mysql → transfer_to_postgres → dbt_run → dbt_test → transform_and_compute_kpis
```

All tasks should turn:

```text
green (SUCCESS)
```

---

## Verifying Results

### MySQL (Staging)

```sql
SELECT COUNT(*) FROM flight_price_staging;

```

---

### PostgreSQL (Analytics)

```sql
SELECT * FROM transformed_flight_prices LIMIT 10;
SELECT * FROM avg_fare_by_airline;
SELECT * FROM booking_count_by_airline;
SELECT * FROM popular_routes;
SELECT * FROM seasonal_fare_variation;

```

---

### Check Data Lineage

```sql
SELECT run_id, COUNT(*) 
FROM transformed_flight_prices
GROUP BY run_id;
```

---

## Logs and Debugging

To view logs:

1. Click a task in Airflow
2. Click **Logs**

---

### Common Issues

| Issue              | Cause                 | Solution                  |
| ------------------ | --------------------- | ------------------------- |
| DAG not visible    | Airflow not restarted | Restart containers        |
| Task fails         | Missing connection    | Check Airflow connections |
| Variable error     | Wrong variable name   | Verify Variables          |
| Connection refused | Container not ready   | Wait until healthy        |

---

## Stopping the System

To stop all services:

```bash
docker compose down
```

---

## Project Structure Overview

```text
dags/        → Airflow DAG definition  
src/         → ETL logic  
data/        → Input dataset  
reports/     → Documentation  
dbt/         → dbt transformation models

```

---

## Conclusion

This guide ensures that users can:

✔ Set up the environment
✔ Configure Airflow
✔ Run the ETL pipeline
✔ Validate results

The pipeline is designed to be **easy to run, monitor, and maintain**.

---
