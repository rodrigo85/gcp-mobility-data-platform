locals {
  datasets = {
    bronze    = "Data as it arrived: Spark (TLC) and Beam (streaming). Schema owned by Terraform."
    silver    = "dbt staging and intermediate models."
    gold      = "dbt facts, dimensions and metrics."
    snapshots = "dbt SCD2 snapshots."
  }
}

resource "google_bigquery_dataset" "layer" {
  for_each = local.datasets

  dataset_id    = each.key
  friendly_name = each.key
  description   = each.value
  location      = var.region

  # Physical (compressed) storage is usually cheaper for large tables
  storage_billing_model = "PHYSICAL"

  depends_on = [google_project_service.enabled]
}

# --- Bronze tables -------------------------------------------------------------
# These are the contract with the producers: Spark writes Parquet with these
# names and types, and Beam writes with CREATE_NEVER. Changing a column is a PR here.

resource "google_bigquery_table" "yellow_trips" {
  dataset_id  = google_bigquery_dataset.layer["bronze"].dataset_id
  table_id    = "yellow_trips"
  description = "TLC trips normalized by pipelines/spark. Loaded by load job, WRITE_TRUNCATE on the month partition."

  time_partitioning {
    type  = "MONTH"
    field = "pickup_datetime"
  }
  clustering               = ["pickup_zone_id"]
  require_partition_filter = false
  deletion_protection      = true

  # Shared with the load task in orchestration/airflow/dags/tlc_monthly_ingestion.py:
  # the load passes this schema explicitly so Parquet timestamps without a time
  # zone land as DATETIME (BigQuery would otherwise infer TIMESTAMP).
  schema = file("${path.module}/../schemas/bronze_yellow_trips.json")
}

resource "google_bigquery_table" "ride_events" {
  dataset_id  = google_bigquery_dataset.layer["bronze"].dataset_id
  table_id    = "ride_events"
  description = "Valid ride events deduplicated by pipelines/beam."

  time_partitioning {
    type  = "DAY"
    field = "event_ts"
  }
  clustering          = ["ride_id"]
  deletion_protection = true

  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "ride_id", type = "STRING", mode = "REQUIRED" },
    { name = "event_type", type = "STRING", mode = "REQUIRED" },
    { name = "event_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    {
      name        = "rider_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Personal identifier: access restricted by policy tag"
      policyTags  = { names = [google_data_catalog_policy_tag.rider_identifier.id] }
    },
    { name = "driver_id", type = "STRING" },
    { name = "pickup_zone_id", type = "INT64", mode = "REQUIRED" },
    { name = "dropoff_zone_id", type = "INT64", mode = "REQUIRED" },
    { name = "fare_amount", type = "FLOAT64" },
    { name = "schema_version", type = "INT64", mode = "REQUIRED" },
    { name = "ingest_ts", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "zone_demand_1m" {
  dataset_id  = google_bigquery_dataset.layer["bronze"].dataset_id
  table_id    = "zone_demand_1m"
  description = "Demand per zone in 1-minute windows. One pane per row (ON_TIME, LATE)."

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
  clustering          = ["pickup_zone_id"]
  deletion_protection = true

  schema = jsonencode([
    { name = "pickup_zone_id", type = "INT64", mode = "REQUIRED" },
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "requested", type = "INT64" },
    { name = "completed", type = "INT64" },
    { name = "cancelled", type = "INT64" },
    { name = "revenue", type = "FLOAT64" },
    { name = "pane_index", type = "INT64" },
    { name = "pane_timing", type = "STRING" },
    { name = "ingest_ts", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "ride_events_dlq" {
  dataset_id  = google_bigquery_dataset.layer["bronze"].dataset_id
  table_id    = "ride_events_dlq"
  description = "Messages rejected by Beam. Kept for 30 days for troubleshooting."

  time_partitioning {
    type          = "DAY"
    field         = "received_ts"
    expiration_ms = 30 * 24 * 60 * 60 * 1000
  }
  deletion_protection = false

  schema = jsonencode([
    { name = "received_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "error", type = "STRING", mode = "REQUIRED" },
    { name = "raw_payload", type = "STRING" },
    { name = "attributes", type = "STRING", description = "Pub/Sub attributes as JSON" },
  ])
}
