locals {
  prefix = "nyc-taxi-${var.env}"
}

# ── Raw bucket: stores original parquet files from TLC ──────
resource "google_storage_bucket" "raw" {
  name          = "${local.prefix}-raw-${var.project_id}"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.env != "prod"

  labels = var.labels

  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days * 4
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# ── Staging bucket: intermediate processing files ────────────
resource "google_storage_bucket" "staging" {
  name          = "${local.prefix}-staging-${var.project_id}"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.env != "prod"

  labels = var.labels

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

# ── Pipeline execution logs bucket ──────────────────────────
resource "google_storage_bucket" "pipeline_logs" {
  name          = "${local.prefix}-pipeline-logs-${var.project_id}"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.env != "prod"

  labels = var.labels

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}
