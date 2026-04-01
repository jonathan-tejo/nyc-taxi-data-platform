# Architecture — NYC Taxi Data Platform

## Overview

The platform follows a **medallion architecture** (raw → bronze → silver → gold) on Google Cloud Platform, fully orchestrated by Google Workflows and provisioned entirely via Terraform.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         NYC Taxi Data Platform                          │
│                                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────────┐  │
│  │  SOURCE  │    │  INGESTION   │    │       STORAGE (GCS)          │  │
│  │          │───▶│              │───▶│                              │  │
│  │ NYC TLC  │    │  ingest.py   │    │  gs://.../yellow/YYYY-MM/    │  │
│  │  (HTTP)  │    │  (Python)    │    │  .parquet (partitioned)      │  │
│  └──────────┘    └──────────────┘    └──────────────┬───────────────┘  │
│                                                      │                  │
│                  ┌───────────────────────────────────▼────────────────┐ │
│                  │              BigQuery                               │ │
│                  │                                                     │ │
│  ┌─────────────┐ │ ┌───────────┐  ┌───────────┐  ┌────────────────┐  │ │
│  │  BRONZE     │ │ │  SILVER   │  │   GOLD    │  │   METADATA     │  │ │
│  │             │─┼▶│           │─▶│           │  │                │  │ │
│  │ trips       │ │ │ trips     │  │ kpi_daily │  │ pipeline_runs  │  │ │
│  │ (raw, typed)│ │ │ dim_zones │  │ kpi_zones │  │ quality_checks │  │ │
│  │             │ │ │           │  │ kpi_hourly│  │                │  │ │
│  └─────────────┘ │ └───────────┘  └─────────-┘  └────────────────┘  │ │
│                  └─────────────────────────────────────────────────────┘ │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                   ORCHESTRATION                                   │  │
│  │                                                                   │  │
│  │  Cloud Scheduler ──▶ Google Workflows                            │  │
│  │                      (main_pipeline.yaml)                        │  │
│  │                      1. Ingest raw data                          │  │
│  │                      2. Load bronze                              │  │
│  │                      3. Quality check bronze                     │  │
│  │                      4. Build silver                             │  │
│  │                      5. Build gold KPIs                          │  │
│  │                      6. Quality gate (block on failure)          │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Layer Definitions

| Layer | Dataset | Description | Partitioning |
|-------|---------|-------------|--------------|
| **Bronze** | `nyc_taxi_{env}_bronze` | Raw TLC data loaded as-is with audit columns. Schema matches source exactly. | MONTH on `tpep_pickup_datetime` |
| **Silver** | `nyc_taxi_{env}_silver` | Cleaned, typed, deduplicated. Rows failing quality rules are excluded. | DAY on `pickup_date` |
| **Gold** | `nyc_taxi_{env}_gold` | Business-ready KPI tables. Pre-aggregated for analytics. | Varies by table |
| **Metadata** | `nyc_taxi_{env}_metadata` | Pipeline runs and quality check results. | DAY |

## Data Flow (per execution)

```
1. Trigger (manual or Cloud Scheduler)
   └─▶ Google Workflows starts

2. Idempotency check
   └─▶ Does gs://raw-bucket/yellow/YYYY-MM/file.parquet exist?
       ├─ Yes: skip download, re-attempt BQ load
       └─ No: download from TLC → upload to GCS

3. Bronze load
   └─▶ BQ load job: GCS parquet → bronze.trips (WRITE_APPEND)

4. Quality check — bronze
   └─▶ not_null, row_count, no_future_dates
   └─▶ FAILED checks → pipeline raises error, Workflows catches

5. Silver transform
   └─▶ Dedup + clean + type cast → silver.trips (CTAS, partitioned)

6. Gold build (parallel-ready)
   ├─▶ kpi_daily_revenue
   ├─▶ kpi_zone_performance
   └─▶ kpi_hourly_patterns

7. Quality gate — gold
   └─▶ Validates minimum row counts and positive revenues
   └─▶ Failure blocks completion

8. Workflow returns: status, run_id, row counts, revenue total
```

## Incremental Strategy

- **Ingest**: parameterized by `execution_date` (YYYY-MM). Re-running the same date is safe — GCS upload is idempotent, BQ load uses `WRITE_APPEND` keyed by `_execution_date`.
- **Silver**: uses `CREATE OR REPLACE TABLE` filtered by `_execution_date`. Re-running rebuilds the partition.
- **Gold**: same pattern — `CREATE OR REPLACE TABLE` per execution date. Safe to re-run.

## Infrastructure (Terraform modules)

```
terraform/
├── main.tf                  Root — wires modules together
├── modules/
│   ├── storage/             GCS buckets (raw, staging, logs)
│   ├── bigquery/            All datasets and metadata tables
│   ├── iam/                 Service account + minimum permissions
│   └── workflows/           Workflow definition + Cloud Scheduler
```

## Observability

- **Structured logs**: every step logs JSON to Cloud Logging via `structlog`.
- **Pipeline runs table**: `metadata.pipeline_runs` — one row per execution with duration, row counts, status.
- **Quality checks table**: `metadata.quality_checks` — all check results queryable in BigQuery.
- **Workflow execution history**: visible in GCP Console → Workflows → Executions.

## Security Model

- One **dedicated service account** per environment with minimum IAM roles.
- No broad project-level editor/owner roles.
- GCS buckets use **Uniform Bucket-Level Access** (no legacy ACLs).
- `terraform.tfvars` is gitignored — secrets never committed.
