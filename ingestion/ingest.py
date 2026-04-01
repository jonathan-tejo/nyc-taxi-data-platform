"""
NYC Taxi Data Ingestion
=======================
Downloads NYC TLC Yellow Taxi parquet files for a given month,
adds audit columns, uploads to GCS raw bucket, and loads into
the BigQuery bronze layer.

Usage:
    python ingest.py --project-id my-project --execution-date 2024-01 --env dev
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import pandas as pd
import requests
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import BQClient, GCSClient, PipelineMetadata, configure_logging

logger = structlog.get_logger(__name__)

# ── TLC data source ──────────────────────────────────────────
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# BQ dataset names follow convention: nyc_taxi_{env}_{layer}
DATASET_BRONZE_TPL = "nyc_taxi_{env}_bronze"
DATASET_METADATA_TPL = "nyc_taxi_{env}_metadata"


def build_tlc_url(execution_date: str) -> str:
    """Construct the TLC parquet URL for a given YYYY-MM date."""
    return f"{TLC_BASE_URL}/yellow_tripdata_{execution_date}.parquet"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
)
def download_parquet(url: str, dest_path: Path) -> int:
    """Download a parquet file from TLC. Returns file size in bytes."""
    logger.info("downloading_file", url=url)
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    total = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
            f.write(chunk)
            total += len(chunk)

    logger.info("download_complete", bytes=total, path=str(dest_path))
    return total


def enrich_dataframe(df: pd.DataFrame, execution_date: str, source_file: str) -> pd.DataFrame:
    """Add audit/lineage columns to the raw dataframe."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    df = df.copy()
    df["_ingested_at"] = ingested_at
    df["_source_file"] = source_file
    df["_execution_date"] = execution_date

    # Normalize column names to match BQ schema (some TLC files have mixed cases)
    df.columns = df.columns.str.strip()
    return df


def validate_raw_dataframe(df: pd.DataFrame, execution_date: str) -> dict:
    """Basic sanity checks before writing to GCS/BQ."""
    issues = []

    if len(df) == 0:
        issues.append("DataFrame is empty — no rows to process")

    if "tpep_pickup_datetime" not in df.columns:
        issues.append("Missing required column: tpep_pickup_datetime")

    if "total_amount" not in df.columns:
        issues.append("Missing required column: total_amount")

    # Check date range makes sense
    if "tpep_pickup_datetime" in df.columns:
        expected_month = execution_date[:7]  # YYYY-MM
        min_date = pd.to_datetime(df["tpep_pickup_datetime"]).min()
        # TLC data sometimes has spurious rows from other months
        in_month = df[
            pd.to_datetime(df["tpep_pickup_datetime"]).dt.strftime("%Y-%m") == expected_month
        ]
        in_month_pct = len(in_month) / len(df)
        if in_month_pct < 0.8:
            issues.append(
                f"Only {in_month_pct:.1%} of rows are within expected month {expected_month}"
            )

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "issues": issues,
        "valid": len(issues) == 0,
    }


@click.command()
@click.option("--project-id", required=True, help="GCP Project ID")
@click.option("--execution-date", required=True, help="Month to ingest, format YYYY-MM")
@click.option(
    "--env",
    default="dev",
    type=click.Choice(["dev", "staging", "prod"]),
    help="Deployment environment",
)
@click.option("--log-level", default="INFO", help="Log level")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Download and validate but do not write to GCS/BQ",
)
def main(
    project_id: str,
    execution_date: str,
    env: str,
    log_level: str,
    dry_run: bool,
) -> None:
    configure_logging(log_level)

    run_id = str(uuid.uuid4())
    log = logger.bind(run_id=run_id, execution_date=execution_date, env=env)

    # Derived names
    raw_bucket = f"nyc-taxi-{env}-raw-{project_id}"
    dataset_bronze = DATASET_BRONZE_TPL.format(env=env)
    dataset_metadata = DATASET_METADATA_TPL.format(env=env)

    gcs = GCSClient(project_id)
    bq = BQClient(project_id)
    metadata = PipelineMetadata(bq, dataset_metadata)

    log.info("ingestion_started", dry_run=dry_run)

    if not dry_run:
        metadata.log_run_start(run_id, execution_date, env)

    try:
        # ── 1. Check idempotency ─────────────────────────────
        gcs_blob_name = f"yellow/{execution_date}/yellow_tripdata_{execution_date}.parquet"
        if gcs.blob_exists(raw_bucket, gcs_blob_name):
            log.info(
                "file_already_exists",
                gcs_path=f"gs://{raw_bucket}/{gcs_blob_name}",
                message="Skipping download — file already in GCS. Use --force to overwrite.",
            )
            # Still proceed to load into BQ in case previous run failed there
        else:
            # ── 2. Download from TLC ─────────────────────────
            url = build_tlc_url(execution_date)
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / f"yellow_tripdata_{execution_date}.parquet"
                file_size = download_parquet(url, local_path)

                # ── 3. Read and validate ─────────────────────
                log.info("reading_parquet", path=str(local_path))
                df = pd.read_parquet(local_path)
                validation = validate_raw_dataframe(df, execution_date)

                log.info(
                    "validation_result",
                    rows=validation["row_count"],
                    valid=validation["valid"],
                    issues=validation["issues"],
                )

                if not validation["valid"]:
                    raise ValueError(
                        f"Raw data validation failed: {validation['issues']}"
                    )

                # ── 4. Enrich with audit columns ─────────────
                df = enrich_dataframe(df, execution_date, gcs_blob_name)

                if dry_run:
                    log.info("dry_run_complete", rows=len(df))
                    return

                # ── 5. Save enriched parquet to temp then GCS ─
                enriched_path = Path(tmpdir) / f"enriched_{execution_date}.parquet"
                df.to_parquet(enriched_path, index=False, engine="pyarrow")

                gcs_uri = gcs.upload_file(
                    str(enriched_path),
                    raw_bucket,
                    gcs_blob_name,
                    content_type="application/octet-stream",
                )
                log.info("uploaded_to_gcs", gcs_uri=gcs_uri, file_size_bytes=file_size)

        # ── 6. Load into BigQuery bronze ─────────────────────
        gcs_uri = f"gs://{raw_bucket}/{gcs_blob_name}"
        log.info("loading_to_bigquery", table=f"{dataset_bronze}.trips")

        load_job = bq.load_parquet_from_gcs(
            gcs_uri=gcs_uri,
            dataset_id=dataset_bronze,
            table_id="trips",
            write_disposition="WRITE_APPEND",
        )

        rows_loaded = load_job.output_rows
        log.info("load_complete", rows_loaded=rows_loaded)

        # ── 7. Log success ───────────────────────────────────
        metadata.log_run_end(
            run_id=run_id,
            execution_date=execution_date,
            status="COMPLETED",
            rows_ingested=rows_loaded,
        )

        # Print summary for Workflows to parse
        import json
        print(
            json.dumps(
                {
                    "status": "COMPLETED",
                    "run_id": run_id,
                    "execution_date": execution_date,
                    "rows_loaded": rows_loaded,
                }
            )
        )

    except Exception as exc:
        log.error("ingestion_failed", error=str(exc), exc_info=True)
        if not dry_run:
            metadata.log_run_end(
                run_id=run_id,
                execution_date=execution_date,
                status="FAILED",
                error_message=str(exc),
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
