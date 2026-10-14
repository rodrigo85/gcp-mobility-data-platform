# One service account per role, each with the minimum it needs.
locals {
  service_accounts = {
    pipelines    = "Beam/Dataflow and simulator: reads Pub/Sub, writes to bronze"
    orchestrator = "Airflow: loads bronze and runs dbt in prod"
    ci           = "GitHub Actions: dbt in ephemeral ci_pr_* datasets"
  }
}

resource "google_service_account" "sa" {
  for_each = local.service_accounts

  account_id   = "mobility-${each.key}"
  display_name = "mobility ${each.key}"
  description  = each.value

  depends_on = [google_project_service.enabled]
}

locals {
  sa_member = { for k, sa in google_service_account.sa : k => "serviceAccount:${sa.email}" }

  project_roles = {
    pipelines-dataflow   = { sa = "pipelines", role = "roles/dataflow.worker" }
    pipelines-jobuser    = { sa = "pipelines", role = "roles/bigquery.jobUser" }
    pipelines-publisher  = { sa = "pipelines", role = "roles/pubsub.publisher" }
    pipelines-subscriber = { sa = "pipelines", role = "roles/pubsub.subscriber" }
    orchestrator-jobuser = { sa = "orchestrator", role = "roles/bigquery.jobUser" }
    # bigquery.user can create datasets (each PR's ci_pr_*) and run jobs
    ci-bq-user = { sa = "ci", role = "roles/bigquery.user" }
  }

  dataset_roles = {
    pipelines-bronze    = { sa = "pipelines", dataset = "bronze", role = "roles/bigquery.dataEditor" }
    orchestrator-bronze = { sa = "orchestrator", dataset = "bronze", role = "roles/bigquery.dataEditor" }
    orchestrator-silver = { sa = "orchestrator", dataset = "silver", role = "roles/bigquery.dataEditor" }
    orchestrator-gold   = { sa = "orchestrator", dataset = "gold", role = "roles/bigquery.dataEditor" }
    orchestrator-snap   = { sa = "orchestrator", dataset = "snapshots", role = "roles/bigquery.dataEditor" }
    ci-bronze-reader    = { sa = "ci", dataset = "bronze", role = "roles/bigquery.dataViewer" }
  }
}

resource "google_project_iam_member" "role" {
  for_each = local.project_roles

  project = var.project_id
  role    = each.value.role
  member  = local.sa_member[each.value.sa]
}

resource "google_bigquery_dataset_iam_member" "role" {
  for_each = local.dataset_roles

  dataset_id = google_bigquery_dataset.layer[each.value.dataset].dataset_id
  role       = each.value.role
  member     = local.sa_member[each.value.sa]
}

resource "google_storage_bucket_iam_member" "lake_writers" {
  for_each = toset(["pipelines", "orchestrator"])

  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = local.sa_member[each.value]
}

# --- Workload Identity Federation for GitHub Actions ---------------------------
# CI exchanges the GitHub OIDC token for short-lived credentials.
# No service account JSON key exists in the repository or in secrets.

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.enabled]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  # Only this repository can use the pool
  attribute_condition = "assertion.repository == \"${var.github_repository}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "ci_wif" {
  service_account_id = google_service_account.sa["ci"].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}
