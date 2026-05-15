import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transfer_mysql_to_postgres_func(mysql_conn_id: str, postgres_conn_id: str, expected_count: int):
    """
    Read validated data from MySQL staging and load it into PostgreSQL
    as a raw table for dbt to transform.
    Returns the row count transferred.
    """
    logging.info("Starting MySQL → PostgreSQL transfer")

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    mysql_conn = mysql_hook.get_conn()
    df = pd.read_sql("SELECT * FROM flight_price_staging;", mysql_conn)
    mysql_conn.close()

    actual_count = len(df)
    logging.info(f"Rows fetched from MySQL: {actual_count} (expected: {expected_count})")

    if actual_count == 0:
        raise ValueError("No data found in MySQL staging table")

    if actual_count != expected_count:
        raise ValueError(f"Row count mismatch: expected {expected_count}, got {actual_count}")

    pg_conn = postgres_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS flight_price_raw ({columns_sql});")
        cursor.execute("DELETE FROM flight_price_raw;")

        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f'"{col}"' for col in df.columns])

        cursor.executemany(
            f"INSERT INTO flight_price_raw ({columns}) VALUES ({placeholders});",
            df.astype(str).values.tolist(),
        )

        pg_conn.commit()
        logging.info(f"Transfer complete. Rows loaded into PostgreSQL: {actual_count}")
        return actual_count

    except Exception as e:
        pg_conn.rollback()
        logging.error("Transfer to PostgreSQL failed")
        raise e

    finally:
        cursor.close()
        pg_conn.close()
