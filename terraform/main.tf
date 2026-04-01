terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Remote state in GCS — replace bucket name before first deploy.
  # Create the bucket manually: gsutil mb gs://<YOUR_PROJECT>-tf-state
  backend "gcs" {
    bucket = "REPLACE_WITH_TF_STATE_BUCKET"
    prefix = "nyc-taxi-platform/tfstate"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  common_labels = merge(
    {
      project     = "nyc-taxi-platform"
      environment = var.env
      managed_by  = "terraform"
    },
    var.labels
  )
}

# ── Modules ─────────────────────────────────────────────────

module "storage" {
  source = "./modules/storage"

  project_id          = var.project_id
  region              = var.region
  env                 = var.env
  labels              = local.common_labels
  data_retention_days = var.data_retention_days
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id   = var.project_id
  bq_location  = var.bq_location
  env          = var.env
  labels       = local.common_labels
}

module "iam" {
  source = "./modules/iam"

  project_id          = var.project_id
  env                 = var.env
  raw_bucket_name     = module.storage.raw_bucket_name
  staging_bucket_name = module.storage.staging_bucket_name
  bq_dataset_bronze   = module.bigquery.dataset_bronze_id
  bq_dataset_silver   = module.bigquery.dataset_silver_id
  bq_dataset_gold     = module.bigquery.dataset_gold_id

  depends_on = [module.storage, module.bigquery]
}

module "workflows" {
  source = "./modules/workflows"

  project_id          = var.project_id
  region              = var.region
  env                 = var.env
  labels              = local.common_labels
  pipeline_sa_email   = module.iam.pipeline_sa_email
  raw_bucket_name     = module.storage.raw_bucket_name
  staging_bucket_name = module.storage.staging_bucket_name
  bq_dataset_bronze   = module.bigquery.dataset_bronze_id
  bq_dataset_silver   = module.bigquery.dataset_silver_id
  bq_dataset_gold     = module.bigquery.dataset_gold_id
  enable_scheduler    = var.enable_scheduler
  scheduler_cron      = var.scheduler_cron

  depends_on = [module.iam]
}
