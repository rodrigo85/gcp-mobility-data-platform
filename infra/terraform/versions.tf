terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.7"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 8.7"
    }
  }

  # After the first apply, migrate the state to a bucket:
  # backend "gcs" {
  #   bucket = "<project-id>-tfstate"
  #   prefix = "mobility"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
  # With user credentials, APIs such as Billing Budgets need a quota project
  user_project_override = true
  billing_project       = var.project_id
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  # With user credentials, APIs such as Billing Budgets need a quota project
  user_project_override = true
  billing_project       = var.project_id
}

data "google_project" "this" {}
