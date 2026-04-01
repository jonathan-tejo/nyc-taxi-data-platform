output "raw_bucket_name" {
  description = "GCS bucket for raw ingested files"
  value       = module.storage.raw_bucket_name
}

output "staging_bucket_name" {
  description = "GCS bucket for staging/intermediate files"
  value       = module.storage.staging_bucket_name
}

output "pipeline_sa_email" {
  description = "Service account email used by the pipeline"
  value       = module.iam.pipeline_sa_email
}

output "dataset_bronze_id" {
  description = "BigQuery dataset ID for bronze layer"
  value       = module.bigquery.dataset_bronze_id
}

output "dataset_silver_id" {
  description = "BigQuery dataset ID for silver layer"
  value       = module.bigquery.dataset_silver_id
}

output "dataset_gold_id" {
  description = "BigQuery dataset ID for gold layer"
  value       = module.bigquery.dataset_gold_id
}

output "workflow_name" {
  description = "Name of the main Google Workflows pipeline"
  value       = module.workflows.workflow_name
}

output "workflow_trigger_command" {
  description = "gcloud command to trigger the pipeline manually"
  value       = "gcloud workflows run ${module.workflows.workflow_name} --location=${var.region} --data='{\"execution_date\": \"YYYY-MM\", \"env\": \"${var.env}\"}'"
}
