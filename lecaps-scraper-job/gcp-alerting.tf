terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>7.0"
    }
  }
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "notification_email" {
  description = "The email address for alert notifications."
  type        = string
}

resource "google_logging_metric" "lecap_scraper_errors" {
  project = var.project_id
  name    = "lecap-scraper-errors"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"lecaps-scraper\" AND severity=ERROR"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_monitoring_notification_channel" "email" {
  project      = var.project_id
  display_name = "Project Owner Email"
  type         = "email"
  labels = {
    email_address = var.notification_email
  }
}

resource "google_monitoring_alert_policy" "lecap_scraper_error_alert" {
  project      = var.project_id
  display_name = "lecap-scraper Error Alert"
  combiner     = "OR"
  conditions {
    display_name = "Log-based metric for lecap-scraper errors"
    condition_threshold {
      filter     = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.lecap_scraper_errors.name}\" AND resource.type=\"cloud_run_revision\""
      duration   = "300s"
      comparison = "COMPARISON_GT"
      threshold_value = 0
      trigger {
        count = 1
      }
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.name]
  documentation {
    content = "This alert triggers when the lecap-scraper Cloud Run service logs an error. Check the service logs for details."
  }
}
