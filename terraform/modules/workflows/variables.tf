variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }
variable "labels" { type = map(string); default = {} }
variable "pipeline_sa_email" { type = string }
variable "raw_bucket_name" { type = string }
variable "staging_bucket_name" { type = string }
variable "bq_dataset_bronze" { type = string }
variable "bq_dataset_silver" { type = string }
variable "bq_dataset_gold" { type = string }
variable "enable_scheduler" { type = bool; default = false }
variable "scheduler_cron" { type = string; default = "0 6 1 * *" }
