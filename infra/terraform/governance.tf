# Column-level security: columns with this policy tag can only be read by
# principals holding roles/datacatalog.categoryFineGrainedReader on the tag.
# Other users get an error when selecting the column (or use SELECT * EXCEPT).

resource "google_data_catalog_taxonomy" "sensitivity" {
  region                 = var.region
  display_name           = "data-sensitivity"
  description            = "Classification of sensitive columns in the mobility platform"
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]

  depends_on = [google_project_service.enabled]
}

resource "google_data_catalog_policy_tag" "rider_identifier" {
  taxonomy     = google_data_catalog_taxonomy.sensitivity.id
  display_name = "rider_identifier"
  description  = "Rider identifier. Access only for the pipeline and authorized investigations."
}

resource "google_data_catalog_policy_tag_iam_member" "rider_identifier_readers" {
  for_each = toset(["pipelines", "orchestrator"])

  policy_tag = google_data_catalog_policy_tag.rider_identifier.name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = local.sa_member[each.value]
}
