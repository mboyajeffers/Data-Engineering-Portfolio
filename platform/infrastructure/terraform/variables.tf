# Terraform Variables
# Author: Mboya Jeffers

variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region for resources"
  type        = string
  default     = "northamerica-northeast2"
}

variable "zone" {
  description = "Google Cloud zone for compute instances"
  type        = string
  default     = "northamerica-northeast2-a"
}

variable "vm_name" {
  description = "Name of the VM instance"
  type        = string
  default     = "analytics-platform-vm"
}

variable "machine_type" {
  description = "Machine type for the VM instance"
  type        = string
  default     = "e2-medium"

  validation {
    condition     = can(regex("^(e2-|n1-|n2-)", var.machine_type))
    error_message = "Machine type must be a valid GCE instance type."
  }
}

variable "boot_disk_size_gb" {
  description = "Size of the boot disk in GB"
  type        = number
  default     = 30

  validation {
    condition     = var.boot_disk_size_gb >= 20 && var.boot_disk_size_gb <= 1000
    error_message = "Boot disk size must be between 20 and 1000 GB."
  }
}

variable "environment" {
  description = "Environment name (production, staging, development)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "staging", "development"], var.environment)
    error_message = "Environment must be production, staging, or development."
  }
}

variable "enable_backup" {
  description = "Enable automatic backups"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 90
}
