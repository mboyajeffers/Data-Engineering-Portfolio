# Terraform Outputs
# Author: Mboya Jeffers

output "vm_name" {
  description = "Name of the production VM"
  value       = google_compute_instance.app_vm.name
}

output "vm_external_ip" {
  description = "External IP address of the VM"
  value       = google_compute_instance.app_vm.network_interface[0].access_config[0].nat_ip
}

output "vm_internal_ip" {
  description = "Internal IP address of the VM"
  value       = google_compute_instance.app_vm.network_interface[0].network_ip
}

output "vm_zone" {
  description = "Zone where the VM is deployed"
  value       = google_compute_instance.app_vm.zone
}

output "gcs_backup_bucket" {
  description = "Name of the backup GCS bucket"
  value       = google_storage_bucket.backups.name
}

output "gcs_reports_bucket" {
  description = "Name of the reports GCS bucket"
  value       = google_storage_bucket.reports.name
}

output "gcs_data_bucket" {
  description = "Name of the data GCS bucket"
  value       = google_storage_bucket.data.name
}

output "app_url" {
  description = "URL to access the web application"
  value       = "http://${google_compute_instance.app_vm.network_interface[0].access_config[0].nat_ip}:8080"
}

output "ssh_command" {
  description = "Command to SSH into the VM"
  value       = "gcloud compute ssh ${google_compute_instance.app_vm.name} --zone=${google_compute_instance.app_vm.zone} --project=${var.project_id}"
}
