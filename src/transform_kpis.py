import logging
from datetime import datetime, timezone

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_and_compute_kpis_func(
    mysql_conn_id: str,
    postgres_conn_id: str,
    expected_count: int
):
    """
    Transform data, compute KPIs, and load into PostgreSQL.
    Uses run_id to preserve history and track lineage.
    """

    logging.info("Starting transformation and KPI computation")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    mysql_conn = mysql_hook.get_conn()
    df = pd.read_sql("SELECT * FROM flight_price_staging;", mysql_conn)
    mysql_conn.close()

    actual_count = len(df)

    logging.info(f"Run ID: {run_id}")
    logging.info(f"Expected validated rows: {expected_count}")
    logging.info(f"Rows fetched from MySQL: {actual_count}")

    if actual_count == 0:
        raise ValueError("No data found in staging table")

    if actual_count != expected_count:
        raise ValueError(
            f"Row count mismatch: expected {expected_count}, got {actual_count}"
        )

    required_cols = [
        "airline",
        "source",
        "destination",
        "base_fare",
        "tax_and_surcharge",
        "total_fare",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["base_fare"] = pd.to_numeric(df["base_fare"], errors="coerce")
    df["tax_and_surcharge"] = pd.to_numeric(df["tax_and_surcharge"], errors="coerce")
    df["total_fare"] = pd.to_numeric(df["total_fare"], errors="coerce")

    df = df.dropna(subset=required_cols)
    df["total_fare"] = df["base_fare"] + df["tax_and_surcharge"]

    transformed_df = df.copy()

    avg_fare_by_airline = (
        df.groupby("airline", as_index=False)["total_fare"]
        .mean()
        .rename(columns={"total_fare": "avg_fare"})
    )

    booking_count_by_airline = (
        df.groupby("airline", as_index=False)
        .size()
        .rename(columns={"size": "booking_count"})
    )

    popular_routes = (
        df.groupby(["source", "destination"], as_index=False)
        .size()
        .rename(columns={"size": "booking_count"})
        .sort_values(by="booking_count", ascending=False)
    )

    seasonal_fare_variation = pd.DataFrame({
        "season_type": ["peak", "non_peak"],
        "avg_fare": [0.0, 0.0],
        "note": [
            "Requires date column",
            "Requires date column"
        ]
    })

    pg_conn = postgres_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_flight_prices (
                run_id TEXT,
                airline TEXT,
                source TEXT,
                destination TEXT,
                base_fare NUMERIC(10,2),
                tax_and_surcharge NUMERIC(10,2),
                total_fare NUMERIC(10,2)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS avg_fare_by_airline (
                run_id TEXT,
                airline TEXT,
                avg_fare NUMERIC(10,2)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS booking_count_by_airline (
                run_id TEXT,
                airline TEXT,
                booking_count INTEGER
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS popular_routes (
                run_id TEXT,
                source TEXT,
                destination TEXT,
                booking_count INTEGER
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seasonal_fare_variation (
                run_id TEXT,
                season_type TEXT,
                avg_fare NUMERIC(10,2),
                note TEXT
            );
        """)

        cursor.executemany(
            """
            INSERT INTO transformed_flight_prices
            (run_id, airline, source, destination, base_fare, tax_and_surcharge, total_fare)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            [
                (run_id, *row)
                for row in transformed_df[[
                    "airline",
                    "source",
                    "destination",
                    "base_fare",
                    "tax_and_surcharge",
                    "total_fare"
                ]].values.tolist()
            ]
        )

        cursor.executemany(
            """
            INSERT INTO avg_fare_by_airline
            (run_id, airline, avg_fare)
            VALUES (%s, %s, %s);
            """,
            [(run_id, *row) for row in avg_fare_by_airline.values.tolist()]
        )

        cursor.executemany(
            """
            INSERT INTO booking_count_by_airline
            (run_id, airline, booking_count)
            VALUES (%s, %s, %s);
            """,
            [(run_id, *row) for row in booking_count_by_airline.values.tolist()]
        )

        cursor.executemany(
            """
            INSERT INTO popular_routes
            (run_id, source, destination, booking_count)
            VALUES (%s, %s, %s, %s);
            """,
            [(run_id, *row) for row in popular_routes.values.tolist()]
        )

        cursor.executemany(
            """
            INSERT INTO seasonal_fare_variation
            (run_id, season_type, avg_fare, note)
            VALUES (%s, %s, %s, %s);
            """,
            [(run_id, *row) for row in seasonal_fare_variation.values.tolist()]
        )

        pg_conn.commit()

        logging.info(f"KPI loading completed successfully for run_id: {run_id}")
        logging.info(f"Transformed rows inserted: {len(transformed_df)}")

        return len(transformed_df)

    except Exception as e:
        pg_conn.rollback()
        logging.error("KPI transformation/loading failed")
        raise e

    finally:
        cursor.close()
        pg_conn.close()