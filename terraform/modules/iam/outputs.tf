output "pipeline_sa_email" {
  value       = google_service_account.pipeline.email
  description = "Email of the pipeline service account"
}

output "pipeline_sa_name" {
  value       = google_service_account.pipeline.name
  description = "Resource name of the pipeline service account"
}
