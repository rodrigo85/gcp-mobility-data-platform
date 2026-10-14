resource "google_storage_bucket" "lake" {
  name     = "${var.project_id}-mobility-lake"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  # The default soft delete (7 days) bills storage for deleted objects.
  # Everything under raw/ is reproducible from TLC, so we don't need it.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age            = var.raw_retention_days
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age            = 1
      matches_prefix = ["dataflow/tmp/", "dataflow/staging/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.enabled]
}
