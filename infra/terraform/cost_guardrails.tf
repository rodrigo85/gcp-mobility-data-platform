# Two layers of cost protection (see docs/adr/0003-bigquery-cost-guardrails.md):
#   1. Budget with alerts: warns, but does not block.
#   2. Daily query-bytes quota: actually blocks.
# The third layer lives in dbt: maximum_bytes_billed per query (dbt/profiles.yml).

resource "google_billing_budget" "monthly" {
  count = var.billing_account_id == "" ? 0 : 1

  billing_account = var.billing_account_id
  display_name    = "mobility-data-platform"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_amount_usd)
    }
  }

  dynamic "threshold_rules" {
    for_each = [0.5, 0.9, 1.0]
    content {
      threshold_percent = threshold_rules.value
    }
  }
}

resource "google_service_usage_consumer_quota_override" "bq_query_usage_per_day" {
  count    = var.enable_query_quota ? 1 : 0
  provider = google-beta

  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project")
  override_value = tostring(var.bq_daily_query_quota_mib)
  force          = true
}
