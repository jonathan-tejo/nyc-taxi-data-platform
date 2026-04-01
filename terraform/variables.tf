variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "env" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.env)
    error_message = "env must be one of: dev, staging, prod."
  }
}

variable "data_retention_days" {
  description = "Days to retain raw files in GCS before lifecycle transition"
  type        = number
  default     = 90
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "enable_scheduler" {
  description = "Whether to create Cloud Scheduler job for automatic pipeline execution"
  type        = bool
  default     = false
}

variable "scheduler_cron" {
  description = "Cron expression for Cloud Scheduler (UTC). Default: 1st of each month at 06:00 UTC"
  type        = string
  default     = "0 6 1 * *"
}

variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default     = {}
}
