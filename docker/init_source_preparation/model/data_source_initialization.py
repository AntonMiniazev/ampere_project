import logging
from importlib.resources import as_file, files

import boto3
import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Connection

from init_source_preparation.config import BootstrapPolicy
from init_source_preparation.db import get_engine, quote, upload_new_data
from init_source_preparation.ddl_init import table_queries
from init_source_preparation.generators.assortment_gen import generate_assortment
from init_source_preparation.generators.clients_gen_init import generate_clients
from init_source_preparation.generators.delivery_resource_gen_init import (
    generate_delivery_resource,
)
from init_source_preparation.logging_utils import APP_NAME
from init_source_preparation.settings import BootstrapSettings, SourceSchema
from init_source_preparation.source_batches import (
    CONTRACT_VERSION,
    CONTROL_TABLES,
    SOURCE_TABLES,
    initialize_source_dataset,
)
from tools.source_batch_staging import StagedChangeBatch

logger = logging.getLogger(APP_NAME)


def _load_dictionary(table_name: str) -> pl.DataFrame:
    dictionaries = files("init_source_preparation.dictionaries")
    with as_file(dictionaries / f"{table_name}.csv") as path:
        return pl.read_csv(path)


def _find_committed_baseline(connection: Connection, target: SourceSchema, run_id: str):
    control_schema = target.control_schema
    # Older installations may predate the initialization run identity column.
    has_run_id = connection.execute(
        text("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema=:control_schema AND table_name='datasets'
              AND column_name='initialization_run_id'
        )
    """),
        {"control_schema": control_schema},
    ).scalar_one()
    if not has_run_id:
        return None

    control = quote(control_schema)
    return (
        connection.execute(
            text(f"""
        SELECT d.dataset_id,d.project_start_date,b.batch_id,b.batch_sequence
        FROM {control}.datasets d JOIN {control}.batches b
          ON b.dataset_id=d.dataset_id AND b.batch_kind='baseline'
         AND b.status='completed'
        WHERE d.initialization_run_id=:run_id AND d.status='ready'
          AND d.source_schema=:source_schema
    """),
            {"run_id": run_id, "source_schema": target.value},
        )
        .mappings()
        .one_or_none()
    )


def _reset_schemas(connection: Connection, target: SourceSchema) -> None:
    source = quote(target.value)
    control = quote(target.control_schema)
    if target is SourceSchema.LEGACY:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {source} CASCADE"))
        connection.execute(text("DROP SCHEMA IF EXISTS source_changes CASCADE"))
        connection.execute(text(f"DROP SCHEMA IF EXISTS {control} CASCADE"))
    else:
        # Drop owned tables together so their foreign keys are allowed. RESTRICT
        # protects any external dependencies and blocks resets if unknown objects exist.
        tables = [
            *(f"{source}.{quote(name)}" for name in SOURCE_TABLES),
            *(f"{control}.{quote(name)}" for name in CONTROL_TABLES),
        ]
        connection.execute(text(f"DROP TABLE IF EXISTS {', '.join(tables)} RESTRICT"))
        connection.execute(text(f"DROP SCHEMA IF EXISTS {source}, {control} RESTRICT"))
    connection.execute(text(f"CREATE SCHEMA {source}"))


def _seed_source(
    connection: Connection, target: SourceSchema, policy: BootstrapPolicy
) -> None:
    start_date = policy.project_start_date.isoformat()
    seeders = {
        "clients": lambda: generate_clients(
            policy.n_of_init_clients, project_start_date=start_date
        ),
        "assortment": generate_assortment,
        "delivery_resource": lambda: generate_delivery_resource(
            policy.n_delivery_resource,
            project_start_date=start_date,
        ),
    }
    for table_name, definition in table_queries.items():
        logger.info("Creating %s.%s", target, table_name)
        connection.execute(text(definition["query"].format(schema=quote(target.value))))
        if definition["type"] == "dict":
            frame = _load_dictionary(table_name)
        elif table_name in seeders:
            frame = seeders[table_name]()
        else:
            continue
        upload_new_data(frame, table_name, target.value, connection=connection)


def initialize_data_sources(
    settings: BootstrapSettings, policy: BootstrapPolicy
) -> None:
    bucket = settings.staging_bucket
    url = settings.postgres_url()
    target = settings.source_schema
    logger.info(
        "Bootstrapping %s.%s at %s:%s (start date=%s, clients=%s, delivery resources=%s, staging=%s/%s)",
        url.database,
        target,
        url.host,
        url.port,
        policy.project_start_date,
        policy.n_of_init_clients,
        policy.n_delivery_resource,
        bucket,
        settings.baseline_prefix,
    )
    client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        verify=settings.minio_ssl_verify,
    )
    staging = {
        "client": client,
        "bucket": bucket,
        "prefix": settings.baseline_prefix,
    }
    engine = get_engine(url)
    try:
        with engine.begin() as connection:
            # Serialize resets and same-run recovery against writers for this target.
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": target.lock_key},
            )
            run_id = settings.initialization_run_id
            if run_id and (
                existing := _find_committed_baseline(connection, target, run_id)
            ):
                tables = tuple(
                    connection.execute(
                        text(
                            f"SELECT source_table FROM {quote(target.control_schema)}.source_tables "
                            "WHERE enabled ORDER BY source_table"
                        )
                    ).scalars()
                )
                StagedChangeBatch.recover_committed(
                    dataset_id=str(existing["dataset_id"]),
                    batch_id=str(existing["batch_id"]),
                    batch_sequence=int(existing["batch_sequence"]),
                    business_date=existing["project_start_date"],
                    contract_version=CONTRACT_VERSION,
                    source_tables=tables,
                    **staging,
                )
                logger.info(
                    "Recovered committed dataset %s for run %s",
                    existing["dataset_id"],
                    run_id,
                )
                return

            _reset_schemas(connection, target)
            _seed_source(connection, target, policy)
            dataset_id, stager = initialize_source_dataset(
                connection,
                target.value,
                policy.project_start_date,
                source_code_version=settings.source_code_version,
                initialization_run_id=run_id,
                control_schema=target.control_schema,
                chunk_rows=settings.staging_chunk_rows,
                target_file_bytes=settings.staging_target_file_bytes,
                **staging,
            )
        # The marker becomes visible only after PostgreSQL has committed.
        stager.publish_committed()
        logger.info("Initialized source dataset %s", dataset_id)
    finally:
        engine.dispose()
