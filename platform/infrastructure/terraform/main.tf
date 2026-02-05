# Production Analytics Platform - Infrastructure
# Author: Mboya Jeffers

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Production VM Instance
resource "google_compute_instance" "app_vm" {
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["http-server", "https-server", "analytics-platform"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = var.boot_disk_size_gb
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Ephemeral external IP
    }
  }

  service_account {
    email  = "default"
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  metadata_startup_script = <<-SCRIPT
    #!/bin/bash
    set -e

    # Update system
    apt-get update && apt-get upgrade -y

    # Install Python 3.12
    add-apt-repository ppa:deadsnakes/ppa -y
    apt-get update
    apt-get install -y python3.12 python3.12-venv python3.12-dev python3-pip

    # Install PostgreSQL 16
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
    apt-get update
    apt-get install -y postgresql-16 postgresql-contrib-16

    # Install Nginx
    apt-get install -y nginx

    # Enable services
    systemctl enable postgresql
    systemctl enable nginx

    # Create application directory
    mkdir -p /opt/app/{engines,services,integrations,logs,data,backups}
    chmod -R 755 /opt/app

    # Install Git
    apt-get install -y git

    echo "VM initialization complete" >> /var/log/startup-script.log
  SCRIPT

  labels = {
    environment = "production"
    application = "analytics-platform"
    managed-by  = "terraform"
  }

  allow_stopping_for_update = true
}
