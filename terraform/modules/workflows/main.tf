locals {
  workflow_name = "nyc-taxi-pipeline-${var.env}"
}

resource "google_workflows_workflow" "main_pipeline" {
  name            = local.workflow_name
  project         = var.project_id
  region          = var.region
  description     = "NYC Taxi end-to-end data pipeline: ingest → bronze → silver → gold → quality checks"
  service_account = var.pipeline_sa_email
  labels          = var.labels

  source_contents = templatefile("${path.module}/workflow_definition.yaml", {
    project_id          = var.project_id
    raw_bucket_name     = var.raw_bucket_name
    staging_bucket_name = var.staging_bucket_name
    bq_dataset_bronze   = var.bq_dataset_bronze
    bq_dataset_silver   = var.bq_dataset_silver
    bq_dataset_gold     = var.bq_dataset_gold
    env                 = var.env
  })
}

# ── Optional: Cloud Scheduler to run monthly ────────────────
resource "google_cloud_scheduler_job" "monthly_trigger" {
  count    = var.enable_scheduler ? 1 : 0
  name     = "nyc-taxi-monthly-trigger-${var.env}"
  project  = var.project_id
  region   = var.region
  schedule = var.scheduler_cron
  time_zone = "UTC"

  description = "Triggers the NYC Taxi pipeline on the 1st of each month"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/${local.workflow_name}/executions"

    body = base64encode(jsonencode({
      argument = jsonencode({
        execution_date = "AUTO"
        env            = var.env
      })
    }))

    oauth_token {
      service_account_email = var.pipeline_sa_email
    }
  }
}
