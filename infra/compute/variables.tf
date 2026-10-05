variable "aws_region" {
  description = "Region for every WikiWatch resource."
  type        = string
  default     = "us-east-1"
}

variable "state_bucket" {
  description = "Terraform state bucket (to read the foundation outputs)."
  type        = string
}

variable "github_repo" {
  description = "Public repository the instance checks out."
  type        = string
  default     = "advaithvemurisai/wikiwatch"
}

variable "repo_tag" {
  description = "Git tag the instance checks out. Required to create the instance; empty is allowed only for destroy."
  type        = string
  default     = ""

  validation {
    # Restrictive on purpose: the value is rendered into the boot script.
    condition     = var.repo_tag == "" || can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+([.-][A-Za-z0-9]+)*$", var.repo_tag))
    error_message = "repo_tag must be a release tag like v0.7.0 or v0.7.0-rc1."
  }
}

variable "market" {
  description = "spot for integration sessions, on-demand for the soak test."
  type        = string
  default     = "spot"

  validation {
    condition     = contains(["spot", "on-demand"], var.market)
    error_message = "market must be spot or on-demand."
  }
}

variable "availability_zone" {
  description = "Zone for the session subnet. Empty: the first zone that offers the instance type. Set another one to retry after a capacity shortage."
  type        = string
  default     = ""

  validation {
    condition     = var.availability_zone == "" || can(regex("^[a-z]{2}-[a-z]+-[0-9][a-z]$", var.availability_zone))
    error_message = "availability_zone must be empty or a zone name like us-east-1b."
  }
}

variable "soak_mode" {
  description = "Soak test: self-shutdown after 48 hours instead of 4."
  type        = bool
  default     = false
}

variable "spark_mem_limit" {
  description = "Spark container limit on the 16 GB instance (the laptop default is 3072m)."
  type        = string
  default     = "6144m"
}

variable "spark_driver_memory" {
  description = "Spark driver heap on the instance (the laptop default is 1536m)."
  type        = string
  default     = "4096m"
}
