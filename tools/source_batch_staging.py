"""Bounded, immutable Parquet change-batch staging on an S3-compatible store.

PostgreSQL transactions own source state and compact batch metadata. This module
only uploads immutable event files and manifests; callers publish the commit
marker after the PostgreSQL transaction commits.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import polars as pl
from botocore.exceptions import ClientError

LOGGER = logging.getLogger("ampere-source-batch")
DEFAULT_PREFIX = "ampere/source-batches/v1"
DEFAULT_BUCKET = "ampere-raw"
DEFAULT_CHUNK_ROWS = 50_000
DEFAULT_TARGET_FILE_BYTES = 128 * 1024 * 1024
_CONTRACT_PATH = Path(__file__).parent / "contracts" / "ampere_tables.json"


def _polars_type(type_text: str) -> pl.DataType:
    simple = {
        "boolean": pl.Boolean,
        "date": pl.Date,
        "smallint": pl.Int16,
        "int": pl.Int32,
        "bigint": pl.Int64,
        "float": pl.Float32,
        "double": pl.Float64,
        "string": pl.String,
        # Business event timestamps are PostgreSQL timestamp-without-time-zone;
        # the technical `_captured_at` field below is explicitly UTC-aware.
        "timestamp": pl.Datetime("us"),
        "timestamp_ntz": pl.Datetime("us"),
    }
    if type_text in simple:
        return simple[type_text]
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", type_text)
    if match:
        return pl.Decimal(precision=int(match.group(1)), scale=int(match.group(2)))
    raise ValueError(f"Unsupported source Parquet contract type: {type_text}")


def _load_business_schemas() -> dict[str, dict[str, pl.DataType]]:
    contract = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    raw_tables = contract["catalog"]["ampere"]["layers"]["raw"]
    schemas = {}
    for table, definition in raw_tables.items():
        types = {column["name"]: column["type_text"] for column in definition["columns"]}
        schemas[table] = {
            name: _polars_type(types[name])
            for name in definition["ingestion"]["business_columns"]
        }
    return schemas


BUSINESS_SCHEMAS = _load_business_schemas()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _put_immutable(client, bucket: str, key: str, body: bytes, sha256: str) -> None:
    """Write once; an existing object is accepted only when its bytes match."""
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if hashlib.sha256(existing).hexdigest() != sha256:
            raise RuntimeError(f"Refusing to replace immutable staging object s3://{bucket}/{key}")
        return
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        Metadata={"sha256": sha256},
    )


def make_s3_client():
    endpoint = os.getenv("MINIO_S3_ENDPOINT") or os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not endpoint or not access_key or not secret_key:
        raise ValueError("MINIO_S3_ENDPOINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required for source staging")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


class StagedChangeBatch:
    """Upload typed change events in bounded Parquet chunks for one source batch."""

    def __init__(
        self,
        *,
        dataset_id: str,
        batch_id: str,
        batch_sequence: int,
        business_date: date,
        contract_version: int,
        source_tables: tuple[str, ...],
        client=None,
        bucket: str | None = None,
        prefix: str | None = None,
        chunk_rows: int | None = None,
        target_file_bytes: int | None = None,
    ) -> None:
        self.dataset_id = str(dataset_id)
        self.batch_id = str(batch_id)
        self.batch_sequence = int(batch_sequence)
        self.business_date = business_date
        self.contract_version = int(contract_version)
        self.source_tables = tuple(source_tables)
        self.client = client or make_s3_client()
        self.bucket = bucket or os.getenv("MINIO_STAGING_BUCKET", DEFAULT_BUCKET)
        self.prefix = (prefix or os.getenv("MINIO_STAGING_PREFIX", DEFAULT_PREFIX)).strip("/")
        self.chunk_rows = int(chunk_rows or os.getenv("AMPERE_STAGING_CHUNK_ROWS", DEFAULT_CHUNK_ROWS))
        self.target_file_bytes = int(
            target_file_bytes or os.getenv("AMPERE_STAGING_TARGET_FILE_BYTES", DEFAULT_TARGET_FILE_BYTES)
        )
        if self.chunk_rows < 1:
            raise ValueError("staging chunk_rows must be positive")
        if self.target_file_bytes < 1:
            raise ValueError("staging target_file_bytes must be positive")
        self._states: dict[str, dict[str, Any]] = {
            name: {"next_change_id": 1, "row_count": 0, "operations": {"B": 0, "I": 0, "U": 0, "D": 0}, "files": []}
            for name in self.source_tables
        }
        self._base = (
            f"{self.prefix}/datasets/{self.dataset_id}/"
            f"batches/{self.batch_sequence:020d}-{self.batch_id}"
        )
        self._prepared = False

    @property
    def row_counts(self) -> dict[str, int]:
        return {table: int(state["row_count"]) for table, state in self._states.items()}

    def append(self, table: str, frame: pl.DataFrame, operation: str) -> int:
        """Stage one operation's exact after-image (or delete image) without buffering it."""
        if self._prepared:
            raise RuntimeError("Cannot append changes after the manifest is prepared")
        if table not in self._states:
            raise ValueError(f"Unregistered source table: {table}")
        if operation not in {"B", "I", "U", "D"}:
            raise ValueError(f"Unsupported source operation: {operation}")
        if frame.is_empty():
            return 0
        if table not in BUSINESS_SCHEMAS:
            raise ValueError(f"Source table is missing from the canonical Raw contract: {table}")
        if "source_row_id" not in frame.columns:
            frame = frame.with_columns(
                pl.Series("source_row_id", [str(uuid4()) for _ in range(frame.height)], dtype=pl.String)
            )
        elif frame.schema["source_row_id"] != pl.String:
            # psycopg returns UUID values as uuid.UUID objects, which Polars
            # represents as Object and cannot cast directly to a Parquet type.
            frame = frame.with_columns(
                pl.Series(
                    "source_row_id",
                    [str(value) for value in frame.get_column("source_row_id").to_list()],
                    dtype=pl.String,
                )
            )

        business_schema = BUSINESS_SCHEMAS[table]
        missing = set(business_schema) - set(frame.columns)
        if missing:
            raise ValueError(f"Mutation image for {table} is missing business columns: {sorted(missing)}")
        frame = frame.with_columns(
            pl.col(name).cast(dtype, strict=True)
            for name, dtype in business_schema.items()
        ).select(*business_schema, "source_row_id")

        state = self._states[table]
        start_id = state["next_change_id"]
        state["next_change_id"] += frame.height
        now = datetime.now(timezone.utc)
        event_frame = frame.with_columns(
            pl.col("source_row_id").cast(pl.String).alias("_source_row_id"),
            pl.lit(self.dataset_id, dtype=pl.String).alias("_dataset_id"),
            pl.lit(self.batch_id, dtype=pl.String).alias("_batch_id"),
            pl.lit(self.batch_sequence, dtype=pl.Int64).alias("_batch_sequence"),
            pl.lit(self.contract_version, dtype=pl.Int32).alias("_contract_version"),
            pl.int_range(start_id, start_id + frame.height, dtype=pl.Int64).alias("_change_id"),
            pl.lit(operation, dtype=pl.String).alias("_op"),
            pl.lit(self.business_date, dtype=pl.Date).alias("_business_date"),
            pl.lit(now, dtype=pl.Datetime(time_zone="UTC")).alias("_captured_at"),
        )
        # Source identity is retained only as lineage. The Raw contract uses the
        # underscored name and does not carry the PostgreSQL helper column.
        event_frame = event_frame.drop("source_row_id")
        written = 0

        def upload_piece(piece: pl.DataFrame, first_id: int) -> int:
            nonlocal state, written
            with tempfile.TemporaryDirectory(prefix="ampere-change-") as temp_dir:
                path = Path(temp_dir) / "part.parquet"
                piece.write_parquet(path, compression="zstd", statistics=True)
                size = path.stat().st_size
                if size > self.target_file_bytes and piece.height > 1:
                    path.unlink()
                    midpoint = piece.height // 2
                    left = upload_piece(piece.slice(0, midpoint), first_id)
                    right = upload_piece(piece.slice(midpoint), first_id + left)
                    return left + right
                checksum = _sha256(path)
                file_index = len(state["files"])
                key = f"{self._base}/{table}/part-{file_index:08d}-{checksum[:16]}.parquet"
                body = path.read_bytes()
                _put_immutable(self.client, self.bucket, key, body, checksum)
                state["files"].append({
                    "key": key,
                    "rows": piece.height,
                    "bytes": len(body),
                    "sha256": checksum,
                    "first_change_id": first_id,
                    "last_change_id": first_id + piece.height - 1,
                })
                return piece.height

        for piece in event_frame.iter_slices(self.chunk_rows):
            written += upload_piece(piece, start_id + written)
        state["row_count"] += written
        state["operations"][operation] += written
        LOGGER.info("STAGED_CHANGE_CHUNK table=%s operation=%s rows=%s files=%s", table, operation, written, len(state["files"]))
        return written

    def prepare(self) -> dict[str, Any]:
        """Publish the prepared manifest, including explicit zero-change tables."""
        if self._prepared:
            raise RuntimeError("Source batch manifest already prepared")
        tables = {}
        for name in self.source_tables:
            state = self._states[name]
            tables[name] = {
                "row_count": state["row_count"],
                "operation_counts": state["operations"],
                "files": state["files"],
                "byte_count": sum(file["bytes"] for file in state["files"]),
            }
        manifest = {
            "manifest_version": 1,
            "contract_version": self.contract_version,
            "dataset_id": self.dataset_id,
            "batch_id": self.batch_id,
            "batch_sequence": self.batch_sequence,
            "business_date": self.business_date.isoformat(),
            "prepared_at": datetime.now(timezone.utc).isoformat(),
            "tables": tables,
        }
        raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        checksum = hashlib.sha256(raw).hexdigest()
        self.manifest_key = f"{self._base}/manifest.json"
        self.manifest_sha256 = checksum
        _put_immutable(self.client, self.bucket, self.manifest_key, raw, checksum)
        self._prepared = True
        LOGGER.info("STAGED_BATCH_PREPARED batch=%s tables=%s bytes=%s", self.batch_id, len(tables), sum(item["byte_count"] for item in tables.values()))
        return manifest

    def publish_committed(self) -> None:
        """Make the prepared payload eligible only after the source DB commits."""
        if not self._prepared:
            raise RuntimeError("Prepare the source batch manifest before publishing it")
        body = json.dumps({
            "manifest_key": self.manifest_key,
            "manifest_sha256": self.manifest_sha256,
            "dataset_id": self.dataset_id,
            "batch_id": self.batch_id,
            "batch_sequence": self.batch_sequence,
            "postgres_committed": True,
            "committed_at": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        key = f"{self._base}/COMMITTED.json"
        self._publish_marker(key, body)
        LOGGER.info("STAGED_BATCH_COMMITTED batch=%s sequence=%s", self.batch_id, self.batch_sequence)

    def _publish_marker(self, key: str, body: bytes) -> None:
        try:
            existing = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
        else:
            marker = json.loads(existing)
            requested = json.loads(body)
            for field in ("manifest_key", "manifest_sha256", "dataset_id", "batch_id", "batch_sequence", "postgres_committed"):
                if marker.get(field) != requested.get(field):
                    raise RuntimeError(f"Existing commit marker does not match batch identity at s3://{self.bucket}/{key}")
            return
        _put_immutable(self.client, self.bucket, key, body, hashlib.sha256(body).hexdigest())

    @classmethod
    def recover_committed(
        cls,
        *,
        dataset_id: str,
        batch_id: str,
        batch_sequence: int,
        business_date: date,
        contract_version: int,
        source_tables: tuple[str, ...],
        client=None,
        bucket: str | None = None,
        prefix: str | None = None,
    ) -> bool:
        """Publish a missing marker for a DB-confirmed completed batch."""
        instance = cls(
            dataset_id=dataset_id,
            batch_id=batch_id,
            batch_sequence=batch_sequence,
            business_date=business_date,
            contract_version=contract_version,
            source_tables=source_tables,
            client=client,
            bucket=bucket,
            prefix=prefix,
        )
        manifest_key = f"{instance._base}/manifest.json"
        try:
            raw = instance.client.get_object(Bucket=instance.bucket, Key=manifest_key)["Body"].read()
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise RuntimeError(
                    f"Completed PostgreSQL batch {batch_id} has no prepared MinIO manifest"
                ) from exc
            raise
        manifest = json.loads(raw)
        manifest_sha256 = hashlib.sha256(raw).hexdigest()
        expected = {
            "contract_version": int(contract_version),
            "dataset_id": str(dataset_id),
            "batch_id": str(batch_id),
            "batch_sequence": int(batch_sequence),
            "business_date": business_date.isoformat(),
        }
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"Prepared manifest identity mismatch for completed batch {batch_id}")
        if set(manifest.get("tables", {})) != set(source_tables):
            raise RuntimeError(f"Prepared manifest for batch {batch_id} does not cover every source table")
        marker = json.dumps({
            "manifest_key": manifest_key,
            "manifest_sha256": manifest_sha256,
            "dataset_id": str(dataset_id),
            "batch_id": str(batch_id),
            "batch_sequence": int(batch_sequence),
            "postgres_committed": True,
            "committed_at": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        instance._publish_marker(f"{instance._base}/COMMITTED.json", marker)
        LOGGER.info("STAGED_BATCH_RECOVERED batch=%s sequence=%s", batch_id, batch_sequence)
        return True
