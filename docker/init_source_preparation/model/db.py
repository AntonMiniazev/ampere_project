import logging

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine, URL

from init_source_preparation.logging_utils import APP_NAME

logger = logging.getLogger(APP_NAME)


def get_engine(url: URL) -> Engine:
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"application_name": APP_NAME},
    )


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def upload_new_data(
    table: pl.DataFrame,
    target_table: str,
    schema: str,
    *,
    connection: Connection,
) -> None:
    if table.is_empty():
        return

    columns = ", ".join(quote(column) for column in table.columns)
    copy_sql = f"COPY {quote(schema)}.{quote(target_table)} ({columns}) FROM STDIN"
    logger.info("Copying %s rows into %s.%s", table.height, schema, target_table)
    raw_connection = connection.connection.driver_connection
    with raw_connection.cursor() as cursor:
        with cursor.copy(copy_sql) as copy:
            for row in table.iter_rows():
                copy.write_row(row)
