output "raw_bucket_name" {
  value = google_storage_bucket.raw.name
}

output "staging_bucket_name" {
  value = google_storage_bucket.staging.name
}

output "pipeline_logs_bucket_name" {
  value = google_storage_bucket.pipeline_logs.name
}
