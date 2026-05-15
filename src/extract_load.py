import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


def load_csv_to_mysql_func(mysql_conn_id: str, csv_path: str, chunksize: int = 1000):
    """
    Extract data from CSV and load into MySQL staging table using batching/chunking.
    Returns total loaded row count for XCom.
    """

    logging.info("Starting CSV ingestion process")

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    total_loaded_rows = 0
    first_chunk = True

    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            logging.info(f"Processing CSV chunk with shape: {chunk.shape}")

            chunk.columns = (
                chunk.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace("&", "and")
                .str.replace("(", "", regex=False)
                .str.replace(")", "", regex=False)
            )

            chunk = chunk.rename(columns={
                "base_fare_bdt": "base_fare",
                "tax_and_surcharge_bdt": "tax_and_surcharge",
                "total_fare_bdt": "total_fare"
            })

            required_columns = [
                "airline",
                "source",
                "destination",
                "base_fare",
                "tax_and_surcharge"
            ]

            missing_cols = [col for col in required_columns if col not in chunk.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")

            chunk["base_fare"] = pd.to_numeric(chunk["base_fare"], errors="coerce")
            chunk["tax_and_surcharge"] = pd.to_numeric(chunk["tax_and_surcharge"], errors="coerce")

            chunk = chunk.dropna(subset=required_columns)

            if "total_fare" not in chunk.columns:
                chunk["total_fare"] = chunk["base_fare"] + chunk["tax_and_surcharge"]
            else:
                chunk["total_fare"] = pd.to_numeric(chunk["total_fare"], errors="coerce")
                chunk["total_fare"] = chunk["total_fare"].fillna(
                    chunk["base_fare"] + chunk["tax_and_surcharge"]
                )

            if chunk.empty:
                logging.warning("Skipping empty chunk after cleaning")
                continue

            if first_chunk:
                columns_sql = ", ".join([f"`{col}` TEXT" for col in chunk.columns])

                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS flight_price_staging (
                    {columns_sql}
                );
                """

                cursor.execute(create_table_sql)

                # Clear staging table only because this is a temporary staging layer.
                # Final PostgreSQL tables should preserve history.
                cursor.execute("DELETE FROM flight_price_staging;")

                first_chunk = False

            placeholders = ", ".join(["%s"] * len(chunk.columns))
            columns = ", ".join([f"`{col}`" for col in chunk.columns])

            insert_sql = f"""
                INSERT INTO flight_price_staging ({columns})
                VALUES ({placeholders});
            """

            cursor.executemany(insert_sql, chunk.astype(str).values.tolist())

            loaded_rows = len(chunk)
            total_loaded_rows += loaded_rows

            logging.info(f"Loaded {loaded_rows} rows in current chunk")

        conn.commit()

        logging.info(f"CSV ingestion completed successfully. Total loaded rows: {total_loaded_rows}")

        if total_loaded_rows == 0:
            raise ValueError("No valid rows were loaded into MySQL staging table")

        return total_loaded_rows

    except Exception as e:
        conn.rollback()
        logging.error("Failed during CSV ingestion")
        raise e

    finally:
        cursor.close()
        conn.close()
