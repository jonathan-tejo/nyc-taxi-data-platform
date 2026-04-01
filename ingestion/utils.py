"""Shared utilities for ingestion: logging, GCS helpers, BQ helpers."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from google.cloud import bigquery, storage
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger(__name__)


# ── Structured logging setup ─────────────────────────────────

def configure_logging(level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            __import__("logging").getLevelName(level)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
    )


# ── GCS helpers ──────────────────────────────────────────────

class GCSClient:
    def __init__(self, project_id: str) -> None:
        self._client = storage.Client(project=project_id)

    def upload_file(
        self,
        local_path: str,
        bucket_name: str,
        blob_name: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path, content_type=content_type)
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logger.info("file_uploaded", gcs_uri=gcs_uri, local_path=local_path)
        return gcs_uri

    def blob_exists(self, bucket_name: str, blob_name: str) -> bool:
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()

    def upload_bytes(
        self,
        data: bytes,
        bucket_name: str,
        blob_name: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{bucket_name}/{blob_name}"


# ── BigQuery helpers ─────────────────────────────────────────

class BQClient:
    def __init__(self, project_id: str) -> None:
        self._client = bigquery.Client(project=project_id)
        self.project_id = project_id

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
    )
    def load_parquet_from_gcs(
        self,
        gcs_uri: str,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        schema_update_options: list[str] | None = None,
    ) -> bigquery.LoadJob:
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
            autodetect=False,
            schema_update_options=schema_update_options or [],
        )

        job = self._client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )

        start = time.monotonic()
        job.result()  # Wait for completion
        elapsed = round(time.monotonic() - start, 2)

        logger.info(
            "bq_load_complete",
            table=table_ref,
            rows=job.output_rows,
            duration_seconds=elapsed,
        )
        return job

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
    def run_query(self, sql: str, params: dict[str, Any] | None = None) -> bigquery.QueryJob:
        query_params = []
        if params:
            for k, v in params.items():
                if isinstance(v, str):
                    query_params.append(bigquery.ScalarQueryParameter(k, "STRING", v))
                elif isinstance(v, int):
                    query_params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
                elif isinstance(v, float):
                    query_params.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        job = self._client.query(sql, job_config=job_config)
        job.result()
        return job

    def insert_rows(self, dataset_id: str, table_id: str, rows: list[dict]) -> None:
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self._client.get_table(table_ref)
        errors = self._client.insert_rows_json(table, rows)
        if errors:
            raise RuntimeError(f"BQ streaming insert errors: {errors}")


# ── Pipeline metadata helpers ────────────────────────────────

class PipelineMetadata:
    def __init__(self, bq: BQClient, dataset_metadata: str) -> None:
        self._bq = bq
        self._dataset = dataset_metadata

    def log_run_start(
        self, run_id: str, execution_date: str, env: str
    ) -> None:
        self._bq.insert_rows(
            self._dataset,
            "pipeline_runs",
            [
                {
                    "run_id": run_id,
                    "execution_date": execution_date,
                    "env": env,
                    "status": "STARTED",
                    "run_date": datetime.now(timezone.utc).isoformat(),
                }
            ],
        )

    def log_run_end(
        self,
        run_id: str,
        execution_date: str,
        status: str,
        rows_ingested: int = 0,
        error_message: str | None = None,
    ) -> None:
        self._bq.insert_rows(
            self._dataset,
            "pipeline_runs",
            [
                {
                    "run_id": run_id,
                    "execution_date": execution_date,
                    "env": "",
                    "status": status,
                    "run_date": datetime.now(timezone.utc).isoformat(),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "rows_ingested": rows_ingested,
                    "error_message": error_message,
                }
            ],
        )
