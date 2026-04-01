locals {
  prefix = "nyc_taxi_${var.env}"
}

# ── Bronze: raw data loaded as-is from GCS ──────────────────
resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = "${local.prefix}_bronze"
  project                    = var.project_id
  location                   = var.bq_location
  description                = "Bronze layer — raw TLC trip data loaded from GCS with minimal transformation"
  delete_contents_on_destroy = var.env != "prod"

  labels = var.labels
}

# ── Silver: cleaned and normalized ──────────────────────────
resource "google_bigquery_dataset" "silver" {
  dataset_id                 = "${local.prefix}_silver"
  project                    = var.project_id
  location                   = var.bq_location
  description                = "Silver layer — cleaned, typed and enriched trips data with dimension tables"
  delete_contents_on_destroy = var.env != "prod"

  labels = var.labels
}

# ── Gold: analytical aggregations and KPIs ──────────────────
resource "google_bigquery_dataset" "gold" {
  dataset_id                 = "${local.prefix}_gold"
  project                    = var.project_id
  location                   = var.bq_location
  description                = "Gold layer — business-ready KPI tables for dashboards and reporting"
  delete_contents_on_destroy = var.env != "prod"

  labels = var.labels
}

# ── Pipeline metadata dataset ────────────────────────────────
resource "google_bigquery_dataset" "metadata" {
  dataset_id                 = "${local.prefix}_metadata"
  project                    = var.project_id
  location                   = var.bq_location
  description                = "Pipeline execution metadata — run logs, quality check results, row counts"
  delete_contents_on_destroy = var.env != "prod"

  labels = var.labels
}

# ── Metadata tables ──────────────────────────────────────────
resource "google_bigquery_table" "pipeline_runs" {
  dataset_id          = google_bigquery_dataset.metadata.dataset_id
  table_id            = "pipeline_runs"
  project             = var.project_id
  deletion_protection = var.env == "prod"

  description = "One row per pipeline execution with status and metrics"

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED", description = "Unique execution ID (Workflow execution name)" },
    { name = "execution_date", type = "STRING", mode = "REQUIRED", description = "Data month being processed (YYYY-MM)" },
    { name = "env", type = "STRING", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED", description = "STARTED | COMPLETED | FAILED" },
    { name = "run_date", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "completed_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "rows_ingested", type = "INTEGER", mode = "NULLABLE" },
    { name = "rows_bronze", type = "INTEGER", mode = "NULLABLE" },
    { name = "rows_silver", type = "INTEGER", mode = "NULLABLE" },
    { name = "rows_gold", type = "INTEGER", mode = "NULLABLE" },
    { name = "duration_seconds", type = "FLOAT", mode = "NULLABLE" },
    { name = "error_message", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "quality_checks" {
  dataset_id          = google_bigquery_dataset.metadata.dataset_id
  table_id            = "quality_checks"
  project             = var.project_id
  deletion_protection = var.env == "prod"

  description = "Data quality check results per pipeline run"

  time_partitioning {
    type  = "DAY"
    field = "check_timestamp"
  }

  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "execution_date", type = "STRING", mode = "REQUIRED" },
    { name = "layer", type = "STRING", mode = "REQUIRED", description = "bronze | silver | gold" },
    { name = "check_name", type = "STRING", mode = "REQUIRED" },
    { name = "table_name", type = "STRING", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED", description = "PASSED | FAILED | WARNING" },
    { name = "rows_tested", type = "INTEGER", mode = "NULLABLE" },
    { name = "rows_failed", type = "INTEGER", mode = "NULLABLE" },
    { name = "failure_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "threshold", type = "FLOAT", mode = "NULLABLE" },
    { name = "check_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "details", type = "STRING", mode = "NULLABLE" }
  ])
}

# ── Bronze table: raw trips ──────────────────────────────────
resource "google_bigquery_table" "bronze_trips" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "trips"
  project             = var.project_id
  deletion_protection = var.env == "prod"

  description = "Raw NYC Yellow Taxi trips — loaded from GCS parquet, no transformations applied"

  time_partitioning {
    type  = "MONTH"
    field = "tpep_pickup_datetime"
  }

  clustering = ["VendorID", "payment_type"]

  schema = jsonencode([
    { name = "VendorID", type = "INTEGER", mode = "NULLABLE" },
    { name = "tpep_pickup_datetime", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "tpep_dropoff_datetime", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "passenger_count", type = "FLOAT", mode = "NULLABLE" },
    { name = "trip_distance", type = "FLOAT", mode = "NULLABLE" },
    { name = "RatecodeID", type = "FLOAT", mode = "NULLABLE" },
    { name = "store_and_fwd_flag", type = "STRING", mode = "NULLABLE" },
    { name = "PULocationID", type = "INTEGER", mode = "NULLABLE" },
    { name = "DOLocationID", type = "INTEGER", mode = "NULLABLE" },
    { name = "payment_type", type = "INTEGER", mode = "NULLABLE" },
    { name = "fare_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "extra", type = "FLOAT", mode = "NULLABLE" },
    { name = "mta_tax", type = "FLOAT", mode = "NULLABLE" },
    { name = "tip_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "tolls_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "improvement_surcharge", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "congestion_surcharge", type = "FLOAT", mode = "NULLABLE" },
    { name = "airport_fee", type = "FLOAT", mode = "NULLABLE" },
    { name = "_ingested_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "_source_file", type = "STRING", mode = "NULLABLE" },
    { name = "_execution_date", type = "STRING", mode = "NULLABLE" }
  ])
}
