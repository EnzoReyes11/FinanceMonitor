output "data_jobs_sa_email" {
  description = "The email of the service account for data pipeline jobs."
  value       = google_service_account.data_jobs_sa.email
}

output "project_id" {
  description = "The ID of the created project."
  value       = google_project.financemonitor.project_id
}

output "artifact_registry_id" {
  description = "The ID of the financemonitor Artifact Registry repository."
  value       = google_artifact_registry_repository.financemonitor_repo.repository_id
}

output "region" {
  description = "The primary region for the project."
  value       = var.region
}
