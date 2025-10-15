terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure the Google Cloud provider
provider "google" {
  # Terraform will automatically use the credentials from your
  # 'gcloud auth application-default login' command.
}

# 1. Create the GCP Project itself (optional if you create it manually)
resource "google_project" "financemonitor" {
  project_id      = var.project_id
  name            = var.project_name
  billing_account = var.billing_account
}

resource "google_project_service" "apis" {
  project = google_project.financemonitor.project_id
  # Use a for_each loop to enable multiple APIs cleanly
  for_each = toset([
    "run.googleapis.com",             # Cloud Run
    "bigquery.googleapis.com",        # BigQuery
    "artifactregistry.googleapis.com",# Artifact Registry (for Docker images)
    "cloudbuild.googleapis.com",      # Cloud Build (for CI/CD)
    "secretmanager.googleapis.com",   # Secret Manager
    "iam.googleapis.com"              # IAM
  ])
  service                = each.key
  disable_on_destroy     = false
}