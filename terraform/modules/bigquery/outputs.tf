output "dataset_bronze_id" {
  value = google_bigquery_dataset.bronze.dataset_id
}

output "dataset_silver_id" {
  value = google_bigquery_dataset.silver.dataset_id
}

output "dataset_gold_id" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "dataset_metadata_id" {
  value = google_bigquery_dataset.metadata.dataset_id
}
