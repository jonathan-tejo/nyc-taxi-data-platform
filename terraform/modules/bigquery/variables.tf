variable "project_id" {
  type = string
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "env" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}
