resource "google_pubsub_topic" "ride_events" {
  name                       = "ride-events"
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.enabled]
}

# Messages Beam cannot ack after several attempts (e.g. a bug that crashes
# the worker) land here instead of blocking the queue.
resource "google_pubsub_topic" "ride_events_dead_letter" {
  name                       = "ride-events-dead-letter"
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.enabled]
}

resource "google_pubsub_subscription" "beam" {
  name  = "ride-events-beam"
  topic = google_pubsub_topic.ride_events.id

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  expiration_policy {
    ttl = "" # never expires
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.ride_events_dead_letter.id
    max_delivery_attempts = 10
  }
}

resource "google_pubsub_subscription" "dead_letter_inspection" {
  name                       = "ride-events-dead-letter-inspection"
  topic                      = google_pubsub_topic.ride_events_dead_letter.id
  message_retention_duration = "604800s"
}

# The Pub/Sub service agent must publish to the dead-letter topic
# and ack messages on the source subscription.
locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  topic  = google_pubsub_topic.ride_events_dead_letter.id
  role   = "roles/pubsub.publisher"
  member = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "dead_letter_subscriber" {
  subscription = google_pubsub_subscription.beam.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
