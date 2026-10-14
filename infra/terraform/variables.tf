variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Single region for everything. us-central1 is in the Cloud Storage free tier."
  type        = string
  default     = "us-central1"
}

variable "github_repository" {
  description = "Repository (owner/name) allowed to impersonate the CI service account via OIDC."
  type        = string
  default     = "rodrigo85/gcp-mobility-data-platform"
}

variable "raw_retention_days" {
  description = "Days before deleting files under raw/ in GCS. The data is already in BigQuery."
  type        = number
  default     = 30
}

variable "billing_account_id" {
  description = "Billing account for the budget alert. Empty disables the budget."
  type        = string
  default     = ""
}

variable "budget_amount" {
  description = "Monthly budget, in budget_currency_code. Alerts at 25/50/75/90/100%."
  type        = number
  default     = 100
}

variable "budget_currency_code" {
  description = "Must match the billing account currency."
  type        = string
  default     = "BRL"
}

variable "enable_kill_switch" {
  description = "Deploys the Cloud Function that unlinks billing when the kill threshold is crossed."
  type        = bool
  default     = true
}

variable "kill_switch_fraction" {
  description = "Fraction of the budget that triggers the kill switch. Below 1.0 because billing data lags by hours."
  type        = number
  default     = 0.7
}

variable "kill_switch_dry_run" {
  description = "Log instead of disabling billing (for testing the alert path)."
  type        = bool
  default     = false
}

variable "enable_query_quota" {
  description = "Applies the custom BigQuery query-bytes-per-day quota."
  type        = bool
  default     = true
}

variable "bq_daily_query_quota_mib" {
  description = <<-EOT
    Daily cap on bytes processed by queries in the project, in MiB.
    30720 MiB/day (30 GiB) adds up to about 0.9 TiB/month, below the 1 TiB free tier.
    The unit is MiB (verified via the Service Usage API: metric unit "MiBy").
  EOT
  type        = number
  default     = 30720
}
