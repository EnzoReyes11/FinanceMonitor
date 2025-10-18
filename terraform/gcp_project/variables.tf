variable "project_id" {
  description = "The desired ID for the new GCP project."
  type        = string
  default     = "financemonitor-prod"
}

variable "project_name" {
  description = "The human-readable name for the project."
  type        = string
  default     = "Finance Monitor Production"
}

variable "billing_account" {
  description = "The ID of the billing account to associate with the project."
  type        = string
  # No default, this should be provided securely
}

variable "org_id" {
  description = "Optional: The ID of the GCP organization this project belongs to."
  type        = string
  default     = null
}

variable "service_account_id" { 
  description = "Service Account for Data Pipeline Jobs"
  type        = string
  default     = "data-pipeline-jobs-sa"
}

variable "region" {
  description = "The primary GCP region for regional resources like Artifact Registry."
  type        = string
}

variable "environment" {
  description = "The running environment"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}
