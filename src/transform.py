import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_and_compute_kpis_func(postgres_conn_id: str, expected_count: int):
    logging.info("Starting final KPI verification")

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()

    try:
        raw_count = pd.read_sql(
            "SELECT COUNT(*) AS count FROM flight_price_raw;",
            conn
        )["count"].iloc[0]

        transformed_count = pd.read_sql(
            "SELECT COUNT(*) AS count FROM transformed_flight_prices;",
            conn
        )["count"].iloc[0]

        logging.info(f"Expected transferred rows: {expected_count}")
        logging.info(f"Raw rows: {raw_count}")
        logging.info(f"Transformed rows: {transformed_count}")

        if raw_count != expected_count:
            raise ValueError(f"Raw count mismatch: expected {expected_count}, got {raw_count}")

        if transformed_count == 0:
            raise ValueError("No rows found in transformed_flight_prices")

        kpi_tables = [
            "avg_fare_by_airline",
            "booking_count_by_airline",
            "popular_routes",
            "seasonal_fare_variation",
        ]

        for table in kpi_tables:
            result = pd.read_sql(
                f"SELECT COUNT(*) AS count FROM {table};",
                conn
            )
            count = result["count"].iloc[0]
            logging.info(f"{table} row count: {count}")

            if count == 0:
                raise ValueError(f"KPI table {table} is empty")

        logging.info("All KPI tables verified successfully")
        return int(transformed_count)

    finally:
        conn.close()