import time
from datetime import datetime

import pandas as pd
from db.mssql import engine
from sqlalchemy import text


def exec_sql(query: str) -> pd.DataFrame | None:
    """
    Execute a SQL query. If it's a SELECT, return DataFrame. If it's DML, return None.
    """
    start = time.perf_counter()
    with engine.begin() as conn:
        result = conn.execute(text(query))
        try:
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            elapsed = time.perf_counter() - start
            print(f"Query (SELECT) executed in {elapsed:.3f} seconds")
            return df
        except Exception:
            elapsed = time.perf_counter() - start
            print(f"Query (DML) executed in {elapsed:.3f} seconds")
            return None


def upload_new_data(
    table: pd.DataFrame, target_table: str, yesterday: datetime.date = None
):
    """
    Upload new data to target_table. Clears delivery_tracking if needed.

    Parameters:
        table: pd.DataFrame to upload
        target_table: name of the table (e.g., 'orders', 'order_product')
        yesterday: datetime.date object used for conditional delete
    """
    if table.empty:
        print(f"No data to upload to {target_table}.")
        return

    if target_table == "delivery_tracking":
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    DELETE FROM [core].[delivery_tracking]
                    WHERE status IS NULL AND order_id IN (
                        SELECT order_id
                        FROM [core].[order_status_history]
                        WHERE order_status_id = 3 AND status_datetime >= '{yesterday}'
                    )
                """)
            )

    table.to_sql(
        name=target_table,
        con=engine,
        schema="core",
        if_exists="append",
        index=False,
    )
    print(f"{len(table)} records inserted into [core].[{target_table}]")


def table_exists(schema: str, table: str) -> bool:
    """
    Check if a table exists in the specified schema.
    Returns True if exists, False otherwise.
    """
    query = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query), {"schema": schema, "table": table}
        ).fetchone()
        return result is not None


def truncate_table(schema: str, table: str):
    """
    Truncate the specified table in the schema.
    """
    query = f"TRUNCATE TABLE [{schema}].[{table}]"
    with engine.begin() as conn:
        conn.execute(text(query))
        print(f"Truncated table: [{schema}].[{table}]")
