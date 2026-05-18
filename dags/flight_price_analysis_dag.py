from airflow.sdk import dag, task, Variable
#from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from src.transform import transform_and_compute_kpis_func

from src.extract_load import load_csv_to_mysql_func
from src.validate import validate_data_mysql_func
from src.transfer import transfer_mysql_to_postgres_func


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
DBT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="flight_price_analysis_v5",
    default_args=default_args,
    description="Flight Price Analysis ETL Pipeline with dbt transformations",
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=["flight-price", "etl", "mysql", "postgres", "dbt"],
)
def flight_price_analysis_dag():

    @task
    def load_csv_to_mysql():
        return load_csv_to_mysql_func(
            mysql_conn_id=MYSQL_CONN_ID,
            csv_path=CSV_PATH,
        )

    @task
    def validate_data_mysql(expected_count: int):
        return validate_data_mysql_func(
            mysql_conn_id=MYSQL_CONN_ID,
            expected_count=expected_count,
        )

    @task
    def transfer_to_postgres(expected_count: int):
        return transfer_mysql_to_postgres_func(
            mysql_conn_id=MYSQL_CONN_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            expected_count=expected_count,
        )
    
    @task
    def transform_and_compute_kpis(expected_count: int):
        return transform_and_compute_kpis_func(
            postgres_conn_id=POSTGRES_CONN_ID,
            expected_count=expected_count,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && ~/.local/bin/dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && ~/.local/bin/dbt test --profiles-dir .",
    )



    loaded_count = load_csv_to_mysql()
    validated_count = validate_data_mysql(loaded_count)
    transferred_count = transfer_to_postgres(validated_count)
    transferred_count >> dbt_run >> dbt_test
    kpi_count = transform_and_compute_kpis(transferred_count)
    dbt_test >> kpi_count


dag_instance = flight_price_analysis_dag()
