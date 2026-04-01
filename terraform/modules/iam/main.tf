locals {
  sa_name = "nyc-taxi-pipeline-${var.env}"
}

# ── Pipeline service account ─────────────────────────────────
resource "google_service_account" "pipeline" {
  account_id   = local.sa_name
  display_name = "NYC Taxi Pipeline SA (${var.env})"
  description  = "Service account used by the data pipeline: ingestion, BQ jobs, Workflows"
  project      = var.project_id
}

# ── GCS permissions ──────────────────────────────────────────
resource "google_storage_bucket_iam_member" "raw_writer" {
  bucket = var.raw_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "staging_writer" {
  bucket = var.staging_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# ── BigQuery permissions ─────────────────────────────────────
# Bronze: full write access (pipeline loads here)
resource "google_bigquery_dataset_iam_member" "bronze_editor" {
  project    = var.project_id
  dataset_id = var.bq_dataset_bronze
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# Silver: full write access
resource "google_bigquery_dataset_iam_member" "silver_editor" {
  project    = var.project_id
  dataset_id = var.bq_dataset_silver
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# Gold: full write access
resource "google_bigquery_dataset_iam_member" "gold_editor" {
  project    = var.project_id
  dataset_id = var.bq_dataset_gold
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# BQ Job User — required to run queries
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Logging — write pipeline logs
resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Monitoring metrics writer
resource "google_project_iam_member" "metrics_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Workflows invoker — Scheduler needs this to trigger Workflows
resource "google_project_iam_member" "workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
