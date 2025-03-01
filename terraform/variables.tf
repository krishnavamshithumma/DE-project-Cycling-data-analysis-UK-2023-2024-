variable "credentials" {
  description = "My credentials"
  default = "./keys/mycreds.json"
}

variable "gcs_bucket" {
  description = "My Storage Bucket"
  default = "yelp-de-project-451206-bucket"
}

variable "project" {
  description = "My project"
  default = "yelp-de-project-451206"
}

variable "location" {
  description = "My location"
  default = "US"
}

variable "region" {
  description = "My region"
  default = "us-central1"
}

variable "google_bigquery_dataset" {
  description = "My Big query dataset"
  default = "yelp_dataset"
}

variable "gcs_storage_class" {
    description = "Bucket storage class"
    default = "STANDARD"
}