# This module defines a single, generic Cloud Run Job
resource "google_cloud_run_v2_job" "this" {
  name     = var.name
  location = var.location
  project  = var.project_id

  template {
    template {
      service_account = var.service_account_email
      containers {
        image = var.container_image
        resources {
          limits = {
            cpu    = var.cpu
            memory = var.memory
          }
        }
      }
    }
  }
}