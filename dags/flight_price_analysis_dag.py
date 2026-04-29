from airflow.sdk import dag, task, Variable
from datetime import datetime, timedelta

from src.extract_load import load_csv_to_mysql_func
from src.validate import validate_data_mysql_func
from src.transform_kpis import transform_and_compute_kpis_func


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 4, 25),
    "catchup": False,
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=15),
}

MYSQL_CONN_ID = Variable.get("mysql_conn_id")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id")
CSV_PATH = Variable.get("csv_path")


@dag(
    dag_id="flight_price_analysis_v5",
    default_args=default_args,
    description="Flight Price Analysis ETL Pipeline using Airflow 3.2.1",
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=["flight-price", "etl", "mysql", "postgres"],
)
def flight_price_analysis_dag():

    @task
    def load_csv_to_mysql():
        loaded_count = load_csv_to_mysql_func(
            mysql_conn_id=MYSQL_CONN_ID,
            csv_path=CSV_PATH,
        )
        return loaded_count

    @task
    def validate_data_mysql(expected_count: int):
        validated_count = validate_data_mysql_func(
            mysql_conn_id=MYSQL_CONN_ID,
            expected_count=expected_count,
        )
        return validated_count

    @task
    def transform_and_compute_kpis(expected_count: int):
        transformed_count = transform_and_compute_kpis_func(
            mysql_conn_id=MYSQL_CONN_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            expected_count=expected_count,
        )
        return transformed_count

    loaded_count = load_csv_to_mysql()
    validated_count = validate_data_mysql(loaded_count)
    transform_and_compute_kpis(validated_count)


dag_instance = flight_price_analysis_dag()