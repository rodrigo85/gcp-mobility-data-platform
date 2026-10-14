output "lake_bucket" {
  value = google_storage_bucket.lake.name
}

output "pubsub_topic" {
  value = google_pubsub_topic.ride_events.id
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.beam.id
}

output "service_accounts" {
  value = { for k, sa in google_service_account.sa : k => sa.email }
}

output "github_wif_provider" {
  description = "Value for workload_identity_provider in google-github-actions/auth"
  value       = google_iam_workload_identity_pool_provider.github.name
}
