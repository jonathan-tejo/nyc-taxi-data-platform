# NYC Taxi Data Platform

A production-grade data engineering platform on **Google Cloud Platform** that ingests, processes, and serves NYC Yellow Taxi trip data across a medallion architecture (bronze → silver → gold). Infrastructure is fully reproducible via **Terraform**; the pipeline is orchestrated by **Google Workflows**.

---

## The Problem

The NYC Taxi & Limousine Commission (TLC) publishes monthly Parquet files with tens of millions of trip records. The goal is to make this data queryable, analytically reliable, and business-ready — with automated ingestion, data quality enforcement, and KPI tables serving dashboards and analysts.

---

## Architecture

```
TLC Public API
     │
     ▼  (Python — ingest.py)
┌──────────────────────┐
│   GCS Raw Bucket     │  ← partitioned by YYYY-MM, idempotent
│   yellow/YYYY-MM/    │
└──────────┬───────────┘
           │ BQ Load Job
           ▼
┌─────────────────────────────────────────────────────────────┐
│                      BigQuery                               │
│                                                             │
│  BRONZE (raw, typed)  →  SILVER (clean)  →  GOLD (KPIs)   │
│                                                             │
│  trips                   trips              kpi_daily_revenue    │
│                          dim_zones          kpi_zone_performance │
│                                             kpi_hourly_patterns  │
└─────────────────────────────────────────────────────────────┘
           ▲
           │  Google Workflows orchestrates every step
           │  Cloud Scheduler triggers monthly (optional)
```

Full architecture details: [docs/architecture.md](docs/architecture.md)

---

## Stack

| Component | Technology |
|-----------|-----------|
| Infrastructure as Code | Terraform >= 1.5 |
| Cloud provider | Google Cloud Platform |
| Data warehouse | BigQuery |
| Object storage | Cloud Storage |
| Orchestration | Google Workflows |
| Scheduling | Cloud Scheduler |
| Ingestion | Python 3.11 |
| Data processing | BigQuery SQL (CTAS) |
| Observability | Cloud Logging + BigQuery metadata tables |

---

## Repository Structure

```
.
├── Makefile                        # All common operations
├── terraform/
│   ├── main.tf                     # Root module — wires all modules
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example    # Copy to terraform.tfvars
│   └── modules/
│       ├── storage/                # GCS buckets
│       ├── bigquery/               # Datasets + metadata tables
│       ├── iam/                    # Service account + IAM bindings
│       └── workflows/              # Workflow definition + Scheduler
│           └── workflow_definition.yaml
├── ingestion/
│   ├── ingest.py                   # Download TLC → GCS → BQ bronze
│   ├── utils.py                    # GCS/BQ helpers, structured logging
│   └── requirements.txt
├── transformations/
│   ├── silver/
│   │   ├── 01_clean_trips.sql      # Dedup + clean + type cast
│   │   └── 02_dim_zones.sql        # Static zone dimension
│   └── gold/
│       ├── 01_kpi_daily_revenue.sql
│       ├── 02_kpi_zone_performance.sql
│       └── 03_kpi_hourly_patterns.sql
├── quality/
│   ├── run_checks.py               # 8-check quality suite
│   └── requirements.txt
└── docs/
    └── architecture.md
```

---

## Data Model

### Bronze — `nyc_taxi_{env}_bronze.trips`

Raw TLC data loaded from GCS Parquet. Schema mirrors TLC source exactly, plus three audit columns: `_ingested_at`, `_source_file`, `_execution_date`.

- **Partition**: MONTH on `tpep_pickup_datetime`
- **Cluster**: `VendorID`, `payment_type`

### Silver — `nyc_taxi_{env}_silver.trips`

Cleaned and enriched. Key transformations:
- Deduplication by `(VendorID, pickup_datetime, dropoff_datetime)`
- Column renaming and type casting
- Derived fields: `trip_duration_min`, `tip_rate`, `is_airport_trip`, `time_of_day_segment`
- Rows with invalid locations, negative amounts, or out-of-range durations are excluded
- Only rows belonging to `execution_date` month are kept

**Partition**: DAY on `pickup_date` | **Cluster**: `pickup_location_id`, `payment_type`

### Silver — `dim_zones`

Static NYC TLC zone lookup (265 zones). Adds `borough_group`, `is_airport` derived fields.

### Gold KPI tables

| Table | Grain | Key metrics |
|-------|-------|-------------|
| `kpi_daily_revenue` | Day | trips, revenue, tip rate, p50/p90/p99 revenue, payment mix |
| `kpi_zone_performance` | Month × Zone | total_pickups, revenue, borough ranking, revenue share |
| `kpi_hourly_patterns` | Month × DOW × Hour | demand index, time-of-day segment, avg revenue |

---

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Terraform >= 1.5 ([install](https://developer.hashicorp.com/terraform/downloads))
- Python 3.11+
- `make` (Linux/Mac) or WSL/Git Bash (Windows)

---

## Deployment

### 1. Enable GCP APIs

```bash
export PROJECT_ID=your-gcp-project-id
make setup-gcp PROJECT_ID=$PROJECT_ID
```

### 2. Create Terraform state bucket

```bash
gsutil mb -p $PROJECT_ID -l us-central1 gs://${PROJECT_ID}-tf-state
```

Update the `backend "gcs"` block in [terraform/main.tf](terraform/main.tf):

```hcl
backend "gcs" {
  bucket = "your-project-tf-state"   # ← change this
  prefix = "nyc-taxi-platform/tfstate"
}
```

### 3. Configure variables

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your project_id and preferences
```

### 4. Deploy infrastructure

```bash
make tf-init
make tf-plan ENV=dev
make tf-apply ENV=dev
```

This creates:
- 3 GCS buckets (raw, staging, pipeline-logs)
- 4 BigQuery datasets (bronze, silver, gold, metadata)
- 2 metadata tables (pipeline_runs, quality_checks)
- 1 bronze trips table (partitioned + clustered)
- 1 service account with minimum IAM permissions
- 1 Google Workflows pipeline

### 5. Install Python dependencies

```bash
make install
```

### 6. Run the pipeline

```bash
# Ingest and process a specific month
make run-pipeline DATE=2024-01

# Or trigger only the ingestion step
make ingest DATE=2024-01

# Run quality checks standalone
make quality-check DATE=2024-01
```

---

## Example Execution

```bash
$ make run-pipeline DATE=2024-03 ENV=dev

→ Triggering pipeline for 2024-03...

Workflow execution started: projects/my-project/locations/us-central1/workflows/nyc-taxi-pipeline-dev/executions/abc123

{
  "status": "COMPLETED",
  "execution_date": "2024-03",
  "total_trips": 3_451_208,
  "total_revenue": 52_847_331.42,
  "duration_seconds": 187
}
```

```bash
$ make quality-check DATE=2024-03

╭──────────┬──────────────────────────────────┬────────┬────────────┬──────────────┬───────────╮
│ Layer    │ Check                            │ Status │ Rows Tested│ Rows Failed  │ Failure % │
├──────────┼──────────────────────────────────┼────────┼────────────┼──────────────┼───────────┤
│ bronze   │ not_null_pickup_datetime         │ PASSED │  3,614,799 │            0 │     0.00% │
│ bronze   │ minimum_row_count                │ PASSED │  3,614,799 │            0 │     0.00% │
│ bronze   │ no_future_pickup_dates           │ PASSED │  3,614,799 │            0 │     0.00% │
│ silver   │ valid_location_ids               │ PASSED │  3,451,208 │           12 │     0.00% │
│ silver   │ non_negative_amounts             │ PASSED │  3,451,208 │            0 │     0.00% │
│ silver   │ trip_duration_range_1_to_300_min │ PASSED │  3,451,208 │         2,18 │     0.06% │
│ gold     │ daily_revenue_completeness       │ PASSED │         31 │            0 │     0.00% │
│ gold     │ positive_daily_revenue           │ PASSED │         31 │            0 │     0.00% │
╰──────────┴──────────────────────────────────┴────────┴────────────┴──────────────┴───────────╯

[PASS] All checks passed (0 warnings).
```

---

## Monitoring & Troubleshooting

### Pipeline execution history

```bash
make pipeline-status
```

Or in GCP Console: **Workflows → nyc-taxi-pipeline-dev → Executions**

### Query pipeline run logs in BigQuery

```sql
SELECT
  execution_date,
  status,
  rows_ingested,
  duration_seconds,
  TIMESTAMP_DIFF(completed_at, run_date, SECOND) AS total_duration_s,
  error_message
FROM `your-project.nyc_taxi_dev_metadata.pipeline_runs`
ORDER BY run_date DESC
LIMIT 20;
```

### Query quality check trends

```sql
SELECT
  execution_date,
  layer,
  check_name,
  status,
  rows_failed,
  failure_rate
FROM `your-project.nyc_taxi_dev_metadata.quality_checks`
WHERE status != 'PASSED'
ORDER BY check_timestamp DESC;
```

### Common issues

| Issue | Likely cause | Fix |
|-------|-------------|-----|
| `404` on TLC download | Month not yet published | TLC publishes ~2 months after the fact. Check available files. |
| BQ load fails schema mismatch | TLC changed column types | Update bronze table schema in `modules/bigquery/main.tf` |
| Quality gate fails row count | Ingestion partially failed | Re-run `make ingest DATE=YYYY-MM` then `make run-pipeline` |
| Workflow permission denied | SA missing a role | Check IAM module, re-apply Terraform |

---

## Technical Decisions

**Why Google Workflows instead of Airflow/Composer?**
Workflows is serverless, has zero infra cost when idle, natively integrates with GCP APIs (BigQuery, GCS), and is simpler to operate. For a monthly batch pipeline, the overhead of Composer is unwarranted.

**Why CTAS for silver/gold instead of MERGE?**
For monthly batch loads, `CREATE OR REPLACE TABLE` on a partition is simpler, cheaper (one pass), and easier to reason about than MERGE. MERGE is reserved for the bronze deduplication step where we need upsert semantics.

**Why partitioning bronze by MONTH but silver by DAY?**
Bronze mirrors the source (monthly files). Silver is queried at day granularity in gold builds, so DAY partitioning avoids full-month scans when building a single day's KPIs during backfills.

**Why a separate metadata dataset?**
Separating observability data from business data makes IAM and cost attribution cleaner. A read-only analyst role on gold doesn't need access to pipeline internals.

---

## Future Improvements

- [ ] **dbt integration** — replace raw SQL files with dbt models for lineage, docs, and testing
- [ ] **Backfill CLI** — `make backfill FROM=2023-01 TO=2024-12` for historical loads
- [ ] **Looker Studio dashboard** — connect to gold tables for a live public demo
- [ ] **Schema evolution** — add BigQuery schema auto-update when TLC adds columns
- [ ] **Green/FHV taxi data** — extend ingestion to other TLC vehicle types
- [ ] **Streaming layer** — add a real-time path via Pub/Sub + Dataflow for live trip data
- [ ] **Cost monitoring** — BigQuery slot and storage cost alerts via Cloud Monitoring

---

## License

MIT
