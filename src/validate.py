import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook


def validate_data_mysql_func(mysql_conn_id: str, expected_count: int):
    """
    Validate and clean MySQL staging data.
    Returns validated row count for XCom.
    """

    logging.info("Starting MySQL staging validation")

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            DELETE FROM flight_price_staging
            WHERE airline IS NULL
               OR source IS NULL
               OR destination IS NULL
               OR TRIM(airline) = ''
               OR TRIM(source) = ''
               OR TRIM(destination) = '';
        """)

        cursor.execute("""
            UPDATE flight_price_staging
            SET
                base_fare = ABS(CAST(base_fare AS DECIMAL(10,2))),
                tax_and_surcharge = ABS(CAST(tax_and_surcharge AS DECIMAL(10,2))),
                total_fare =
                    ABS(CAST(base_fare AS DECIMAL(10,2)))
                    + ABS(CAST(tax_and_surcharge AS DECIMAL(10,2)))
            WHERE
                CAST(base_fare AS DECIMAL(10,2)) < 0
                OR CAST(tax_and_surcharge AS DECIMAL(10,2)) < 0
                OR total_fare IS NULL
                OR TRIM(total_fare) = ''
                OR CAST(total_fare AS DECIMAL(10,2)) < 0;
        """)

        cursor.execute("SELECT COUNT(*) FROM flight_price_staging;")
        validated_count = cursor.fetchone()[0]

        logging.info(f"Expected rows from load task: {expected_count}")
        logging.info(f"Validated rows in MySQL staging: {validated_count}")

        if validated_count == 0:
            raise ValueError("Validation failed: no rows remain after cleaning")

        if validated_count > expected_count:
            raise ValueError(
                f"Validation failed: validated count {validated_count} is greater than loaded count {expected_count}"
            )

        conn.commit()
        logging.info("MySQL staging validation completed successfully")

        return validated_count

    except Exception as e:
        conn.rollback()
        logging.error("Validation failed")
        raise e

    finally:
        cursor.close()
        conn.close()