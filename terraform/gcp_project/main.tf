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

# Create the GCP Project itself (optional if you create it manually)
resource "google_project" "financemonitor" {
  project_id      = var.project_id
  name            = var.project_name
  billing_account = var.billing_account
}

resource "google_project_service" "apis" {
  project = google_project.financemonitor.project_id

  for_each = toset([
    "run.googleapis.com",             # Cloud Run
    "bigquery.googleapis.com",
    "artifactregistry.googleapis.com",# Artifact Registry (for Docker images)
    "cloudbuild.googleapis.com",      # Cloud Build (for CI/CD)
    "secretmanager.googleapis.com",
    "iam.googleapis.com"
  ])
  service                = each.key
  disable_on_destroy     = false
}

resource "google_service_account" "data_jobs_sa" {
  project      = google_project.financemonitor.project_id
  account_id   = var.service_account_id
  display_name = "Service Account for Data Pipeline Jobs"
}

# --- Create the Artifact Registry ---
resource "google_artifact_registry_repository" "financemonitor_repo" {
  project       = google_project.financemonitor.project_id
  location      = var.region
  repository_id = "financemonitor"
  description   = "Docker repository for financemonitor services"
  format        = "DOCKER"
}

# --- Grant Cloud Build permissions to the new repository ---
data "google_project" "project" {
  project_id = google_project.financemonitor.project_id
}

resource "google_artifact_registry_repository_iam_member" "build_pusher" {
  project    = google_artifact_registry_repository.financemonitor_repo.project
  location   = google_artifact_registry_repository.financemonitor_repo.location
  repository = google_artifact_registry_repository.financemonitor_repo.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}
