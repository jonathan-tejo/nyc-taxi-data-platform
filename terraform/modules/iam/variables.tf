variable "project_id" {
  type = string
}

variable "env" {
  type = string
}

variable "raw_bucket_name" {
  type = string
}

variable "staging_bucket_name" {
  type = string
}

variable "bq_dataset_bronze" {
  type = string
}

variable "bq_dataset_silver" {
  type = string
}

variable "bq_dataset_gold" {
  type = string
}
