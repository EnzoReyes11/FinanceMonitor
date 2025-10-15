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
