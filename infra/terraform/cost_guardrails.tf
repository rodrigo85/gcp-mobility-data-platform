# Hard spending cap (see docs/adr/0003-bigquery-cost-guardrails.md):
#   1. Budget with e-mail alerts: warns, does not block.
#   2. Kill switch: the budget publishes to Pub/Sub; a Cloud Function unlinks the
#      billing account once spend crosses kill_switch_fraction of the budget.
#      Billing data lags by hours, so the switch fires below 100% on purpose.
#   3. Daily query-bytes quota on BigQuery: blocks queries immediately.
# A fourth layer lives in dbt: maximum_bytes_billed per query (dbt/profiles.yml).

locals {
  budget_enabled      = var.billing_account_id != ""
  kill_switch_enabled = local.budget_enabled && var.enable_kill_switch
}

resource "google_pubsub_topic" "budget_alerts" {
  count = local.budget_enabled ? 1 : 0

  name       = "budget-alerts"
  depends_on = [google_project_service.enabled]
}

# Cloud Billing publishes budget notifications with this Google-managed identity
resource "google_pubsub_topic_iam_member" "budget_alerts_publisher" {
  count = local.budget_enabled ? 1 : 0

  topic  = google_pubsub_topic.budget_alerts[0].id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:billing-budget-alert@system.gserviceaccount.com"
}

resource "google_billing_budget" "monthly" {
  count = local.budget_enabled ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "mobility-data-platform"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
    # Count gross cost: free-tier credits must not hide real spend
    credit_types_treatment = "EXCLUDE_ALL_CREDITS"
  }

  amount {
    specified_amount {
      currency_code = var.budget_currency_code
      units         = tostring(var.budget_amount)
    }
  }

  dynamic "threshold_rules" {
    for_each = [0.25, 0.5, 0.75, 0.9, 1.0]
    content {
      threshold_percent = threshold_rules.value
    }
  }

  all_updates_rule {
    pubsub_topic   = google_pubsub_topic.budget_alerts[0].id
    schema_version = "1.0"
    # Keep the default e-mails to billing admins in addition to Pub/Sub
    disable_default_iam_recipients = false
  }

  depends_on = [google_pubsub_topic_iam_member.budget_alerts_publisher]
}

# --- Kill switch ---------------------------------------------------------------

resource "google_service_account" "kill_switch" {
  count = local.kill_switch_enabled ? 1 : 0

  account_id   = "mobility-budget-kill-switch"
  display_name = "mobility budget kill switch"
  description  = "Unlinks the billing account when the budget kill threshold is crossed"
}

# Project Billing Manager can unlink billing from this project, and nothing else
resource "google_project_iam_member" "kill_switch_billing" {
  count = local.kill_switch_enabled ? 1 : 0

  project = var.project_id
  role    = "roles/billing.projectManager"
  member  = "serviceAccount:${google_service_account.kill_switch[0].email}"
}

data "archive_file" "kill_switch_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/budget_kill_switch"
  output_path = "${path.module}/.build/budget_kill_switch.zip"
  excludes    = ["test_main.py", "__pycache__"]
}

resource "google_storage_bucket_object" "kill_switch_source" {
  count = local.kill_switch_enabled ? 1 : 0

  # Content hash in the name: a code change forces a redeploy
  name   = "functions/budget_kill_switch-${data.archive_file.kill_switch_source.output_md5}.zip"
  bucket = google_storage_bucket.lake.name
  source = data.archive_file.kill_switch_source.output_path
}

resource "google_cloudfunctions2_function" "kill_switch" {
  count = local.kill_switch_enabled ? 1 : 0

  name     = "budget-kill-switch"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "handle_budget_alert"
    source {
      storage_source {
        bucket = google_storage_bucket.lake.name
        object = google_storage_bucket_object.kill_switch_source[0].name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.kill_switch[0].email
    environment_variables = {
      KILL_SWITCH_FRACTION = tostring(var.kill_switch_fraction)
      DRY_RUN              = tostring(var.kill_switch_dry_run)
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.budget_alerts[0].id
    retry_policy   = "RETRY_POLICY_RETRY"
  }

  depends_on = [google_project_iam_member.kill_switch_billing]
}

# --- BigQuery query quota ----------------------------------------------------

resource "google_service_usage_consumer_quota_override" "bq_query_usage_per_day" {
  count    = var.enable_query_quota ? 1 : 0
  provider = google-beta

  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project")
  override_value = tostring(var.bq_daily_query_quota_mib)
  force          = true

  depends_on = [google_project_service.enabled]
}
