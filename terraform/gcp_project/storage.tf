resource "google_storage_bucket" "financemonitor_data" {
  project       = var.project_id
  name          = "${var.project_id}-financemonitor-data"  # Must be globally unique
  location      = var.region
  
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 45  # days
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"  # Move old data to cheaper storage
    }
  }
  
  lifecycle_rule {
    condition {
      age = 100  # days
    }
    action {
      type = "Delete"
    }
  }
  
  # Prevent accidental deletion
  force_destroy = false  # Set to true only in dev
  
  # Labels for organization
  labels = {
    environment = var.environment
    purpose     = "stock-data"
    managed_by  = "terraform"
  }
}

# Grant the service account access to the bucket
resource "google_storage_bucket_iam_member" "data_jobs_writer" {
  bucket = google_storage_bucket.financemonitor_data.name
  role   = "roles/storage.objectAdmin"  # Read and write objects
  member = "serviceAccount:${google_service_account.data_jobs_sa.email}"
}

# Optional: Grant Cloud Run service accounts access
resource "google_storage_bucket_iam_member" "cloud_run_writer" {
  bucket = google_storage_bucket.financemonitor_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

# Output the bucket name for use in other resources
output "data_bucket_name" {
  value       = google_storage_bucket.financemonitor_data.name
  description = "GCS bucket for finance data"
}

output "data_bucket_url" {
  value       = "gs://${google_storage_bucket.financemonitor_data.name}"
  description = "GCS bucket URI"
}