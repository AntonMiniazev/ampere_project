from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload dbt artifacts to the silver ops bucket."
    )
    parser.add_argument(
        "--artifacts-dir",
        default="/app/dbt/target",
        help="Directory that contains dbt artifact files.",
    )
    parser.add_argument(
        "--log-file",
        default="/app/logs/dbt.log",
        help="Optional dbt log file to upload with the artifacts.",
    )
    return parser.parse_args()


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Unsupported S3 URI: {uri}")
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix.rstrip("/")


def main() -> None:
    args = parse_args()
    artifacts_dir = Path(args.artifacts_dir)
    log_file = Path(args.log_file)
    upload_root = os.getenv("SILVER_DBT_ARTIFACT_ROOT", "s3://ampere-silver-ops/dbt")
    logical_date = os.getenv("LOGICAL_DATE", "unknown-date")

    run_results = json.loads((artifacts_dir / "run_results.json").read_text(encoding="utf-8"))
    invocation_id = run_results.get("metadata", {}).get("invocation_id", "unknown-invocation")
    bucket, prefix = split_s3_uri(upload_root)
    target_prefix = f"{prefix}/logical_date={logical_date}/invocation_id={invocation_id}".strip("/")

    endpoint = os.getenv("MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000")
    use_ssl = endpoint.startswith("https://")
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name="us-east-1",
        use_ssl=use_ssl,
    )

    upload_files = [
        artifacts_dir / "manifest.json",
        artifacts_dir / "run_results.json",
        artifacts_dir / "sources.json",
    ]
    if log_file.exists():
        upload_files.append(log_file)

    uploaded: list[str] = []
    for file_path in upload_files:
        if not file_path.exists():
            continue
        object_name = f"{target_prefix}/{file_path.name}"
        s3_client.upload_file(str(file_path), bucket, object_name)
        uploaded.append(f"s3://{bucket}/{object_name}")

    if not uploaded:
        raise SystemExit("No dbt artifacts were uploaded.")

    print("Uploaded dbt artifacts:")
    for uri in uploaded:
        print(uri)


if __name__ == "__main__":
    main()
