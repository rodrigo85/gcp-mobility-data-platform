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

variable "budget_amount_usd" {
  description = "Monthly budget in USD. Alerts at 50%, 90% and 100%."
  type        = number
  default     = 1
}

variable "enable_query_quota" {
  description = "Applies the custom BigQuery query-bytes-per-day quota."
  type        = bool
  default     = false
}

variable "bq_daily_query_quota_mib" {
  description = <<-EOT
    Daily cap on bytes processed by queries in the project, in MiB.
    30720 MiB/day (30 GiB) adds up to about 0.9 TiB/month, below the 1 TiB free tier.
    Check the unit in the console (IAM > Quotas > "Query usage per day") before enabling.
  EOT
  type        = number
  default     = 30720
}
