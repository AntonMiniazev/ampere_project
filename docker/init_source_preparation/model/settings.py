from datetime import date
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import (
    AliasChoices,
    Field,
    NonNegativeInt,
    PositiveInt,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

from tools.source_batch_staging import (
    DEFAULT_BUCKET,
    DEFAULT_CHUNK_ROWS,
    DEFAULT_PREFIX,
    DEFAULT_TARGET_FILE_BYTES,
)


class SourceSchema(StrEnum):
    LEGACY = "source"
    GENERATOR_V2 = "ampere_source"

    @property
    def control_schema(self) -> str:
        return "pipeline" if self is SourceSchema.LEGACY else "ampere_pipeline"

    @property
    def lock_key(self) -> int:
        # The legacy key is shared with the active daily and backfill generator.
        return 724614902631 if self is SourceSchema.LEGACY else 724614902632


class BootstrapSettings(BaseSettings):
    model_config = SettingsConfigDict(
        frozen=True,
        case_sensitive=True,
        env_ignore_empty=True,
        hide_input_in_errors=True,
    )

    reset_mode: Literal["reset"] = Field(validation_alias="SOURCE_INIT_MODE")
    source_schema: SourceSchema = Field(
        SourceSchema.LEGACY, validation_alias="SCHEMA_INIT"
    )
    n_of_init_clients: NonNegativeInt | None = Field(
        None, validation_alias="N_OF_INIT_CLIENTS"
    )
    n_delivery_resource: NonNegativeInt | None = Field(
        None, validation_alias="N_DELIVERY_RESOURCE"
    )
    project_start_date: date | None = Field(None, validation_alias="PROJECT_START_DATE")
    initialization_run_id: str | None = Field(
        None,
        validation_alias=AliasChoices(
            "AMPERE_BOOTSTRAP_RUN_ID", "AMPERE_AIRFLOW_RUN_ID"
        ),
    )
    source_code_version: str = Field(
        "unknown", validation_alias="AMPERE_SOURCE_CODE_VERSION"
    )

    database_url: SecretStr | None = Field(None, validation_alias="DATABASE_URL")
    pg_user: str | None = Field(None, validation_alias=AliasChoices("PGUSER", "pguser"))
    pg_password: SecretStr | None = Field(
        None, validation_alias=AliasChoices("PGPASSWORD", "pgpass")
    )
    pg_host: str = Field("postgres-service", validation_alias="PGHOST")
    pg_port: Annotated[int, Field(ge=1, le=65535)] = Field(
        5432, validation_alias="PGPORT"
    )
    pg_database: str = Field("ampere_db", validation_alias="PGDATABASE")

    minio_endpoint: str = Field(
        validation_alias=AliasChoices("MINIO_S3_ENDPOINT", "MINIO_ENDPOINT")
    )
    minio_access_key: SecretStr = Field(validation_alias="MINIO_ACCESS_KEY")
    minio_secret_key: SecretStr = Field(validation_alias="MINIO_SECRET_KEY")
    minio_region: str = Field("us-east-1", validation_alias="MINIO_REGION")
    minio_ssl_verify: bool | None = Field(None, validation_alias="MINIO_SSL_VERIFY")
    staging_bucket: str = Field(DEFAULT_BUCKET, validation_alias="MINIO_STAGING_BUCKET")
    staging_prefix: str = Field(DEFAULT_PREFIX, validation_alias="MINIO_STAGING_PREFIX")
    staging_chunk_rows: PositiveInt = Field(
        DEFAULT_CHUNK_ROWS, validation_alias="AMPERE_STAGING_CHUNK_ROWS"
    )
    staging_target_file_bytes: PositiveInt = Field(
        DEFAULT_TARGET_FILE_BYTES,
        validation_alias="AMPERE_STAGING_TARGET_FILE_BYTES",
    )

    @field_validator("reset_mode", mode="before")
    @classmethod
    def normalize_reset_mode(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("project_start_date", mode="before")
    @classmethod
    def parse_project_date(cls, value: object) -> object:
        return date.fromisoformat(value) if isinstance(value, str) else value

    @field_validator("staging_prefix")
    @classmethod
    def normalize_staging_prefix(cls, value: str) -> str:
        prefix = value.strip("/")
        if not prefix or any(part in {"", ".", ".."} for part in prefix.split("/")):
            raise ValueError("MINIO_STAGING_PREFIX must be a non-empty object path")
        return prefix

    @model_validator(mode="after")
    def protect_raw_bucket(self) -> Self:
        if (
            self.source_schema is SourceSchema.GENERATOR_V2
            and self.staging_bucket.strip()
            in {
                "",
                DEFAULT_BUCKET,
            }
        ):
            raise ValueError(
                "Set MINIO_STAGING_BUCKET to a dedicated bucket for ampere_source "
                f"({DEFAULT_BUCKET} is reserved for the main pipeline)"
            )
        return self

    @property
    def baseline_prefix(self) -> str:
        if self.source_schema is SourceSchema.GENERATOR_V2:
            # Legacy Raw reads and cleans only <prefix>/datasets/.
            return f"{self.staging_prefix}/ampere_source"
        return self.staging_prefix

    def postgres_url(self) -> URL:
        url = URL.create("postgresql+psycopg")
        if self.database_url is not None:
            try:
                url = make_url(self.database_url.get_secret_value())
            except (ArgumentError, ValueError):
                raise ValueError(
                    "DATABASE_URL must be a valid PostgreSQL URL"
                ) from None
            if url.drivername not in {"postgresql", "postgresql+psycopg"}:
                raise ValueError("DATABASE_URL must use PostgreSQL with psycopg")
        username = url.username or self.pg_user
        password = url.password or (
            self.pg_password.get_secret_value() if self.pg_password else None
        )
        if not username or not password:
            raise ValueError(
                "Set PGUSER and PGPASSWORD or include both in DATABASE_URL"
            )
        return url.set(
            drivername="postgresql+psycopg",
            username=username,
            password=password,
            host=url.host or self.pg_host,
            port=url.port or self.pg_port,
            database=url.database or self.pg_database,
        )
