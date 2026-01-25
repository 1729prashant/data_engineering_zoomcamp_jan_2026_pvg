variable "credentials" {
  description = "Path to the Google Cloud Service Account JSON key file"
  type        = string
}


variable "project" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1" # You can keep defaults for things that rarely change
}


variable "location" {
  description = "Project Location (Multi-region or Region)"
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}