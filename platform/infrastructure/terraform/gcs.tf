# Cloud Storage Buckets
# Author: Mboya Jeffers

# Backup storage bucket
resource "google_storage_bucket" "backups" {
  name          = "${var.vm_name}-backups-${var.project_id}"
  location      = "NORTHAMERICA-NORTHEAST2"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age            = 30
      num_newer_versions = 3
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    purpose     = "backups"
    managed-by  = "terraform"
  }
}

# Reports storage bucket
resource "google_storage_bucket" "reports" {
  name          = "${var.vm_name}-reports-${var.project_id}"
  location      = "NORTHAMERICA-NORTHEAST2"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD"]
    response_header = ["*"]
    max_age_seconds = 3600
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    purpose     = "reports"
    managed-by  = "terraform"
  }
}

# Data storage bucket
resource "google_storage_bucket" "data" {
  name          = "${var.vm_name}-data-${var.project_id}"
  location      = "NORTHAMERICA-NORTHEAST2"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    purpose     = "data-storage"
    managed-by  = "terraform"
  }
}
