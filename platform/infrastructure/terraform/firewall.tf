# Network Security Rules
# Author: Mboya Jeffers

# Allow HTTP traffic
resource "google_compute_firewall" "allow_http" {
  name    = "${var.vm_name}-allow-http"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["http-server"]
  description   = "Allow HTTP traffic"
}

# Allow HTTPS traffic
resource "google_compute_firewall" "allow_https" {
  name    = "${var.vm_name}-allow-https"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["https-server"]
  description   = "Allow HTTPS traffic"
}

# Allow SSH traffic
resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.vm_name}-allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  description   = "Allow SSH access"
}

# Allow application web interface
resource "google_compute_firewall" "allow_app" {
  name    = "${var.vm_name}-allow-app"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["analytics-platform"]
  description   = "Allow application web interface"
}

# Deny RDP explicitly
resource "google_compute_firewall" "deny_rdp" {
  name     = "${var.vm_name}-deny-rdp"
  network  = "default"
  priority = 1000

  deny {
    protocol = "tcp"
    ports    = ["3389"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["analytics-platform"]
  description   = "Explicitly deny RDP access"
}

# Allow internal PostgreSQL traffic only
resource "google_compute_firewall" "allow_postgres_internal" {
  name    = "${var.vm_name}-allow-postgres-internal"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }

  source_ranges = ["10.0.0.0/8"]
  target_tags   = ["analytics-platform"]
  description   = "Allow internal-only PostgreSQL connections"
}
