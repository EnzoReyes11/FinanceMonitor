terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Read the output from the LOCAL gcp_project state file
data "terraform_remote_state" "gcp_project" {
  backend = "local"
  config = {
    path = "../gcp_project/terraform.tfstate"
  }
}

locals {
  project_id            = data.terraform_remote_state.gcp_project.outputs.project_id
  region                = data.terraform_remote_state.gcp_project.outputs.region
  service_account_email = data.terraform_remote_state.gcp_project.outputs.data_jobs_sa_email
  artifact_registry_id  = data.terraform_remote_state.gcp_project.outputs.artifact_registry_id
}

provider "google" {
  project = local.project_id
  region  = local.region
}

# Grant necessary permissions to the data jobs service account ---
resource "google_project_iam_member" "bq_writer" {
  project = local.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${local.service_account_email}"
}

resource "google_project_iam_member" "gcs_manager" {
  project = local.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${local.service_account_email}"
}

resource "google_project_iam_member" "run_invoker" {
  project = local.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${local.service_account_email}"
}

# Loop over the services defined in your variables and create a Cloud Run job for each one.
module "cloud_run_jobs" {
  source   = "./modules/cloud_run_job"
  for_each = var.services # This loops through the 'services' map in your .tfvars file

  project_id            = local.project_id
  location              = local.region
  name                  = each.key
  service_account_email = local.service_account_email
  memory                = each.value.memory

  container_image = format("%s-docker.pkg.dev/%s/%s/%s:latest",
    local.region,
    local.project_id,
    local.artifact_registry_id,
    each.value.image_name
  )
}
