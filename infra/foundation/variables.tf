variable "aws_region" {
  description = "Region for every WikiWatch resource."
  type        = string
  default     = "us-east-1"
}

variable "state_bucket" {
  description = "Terraform state bucket from the bootstrap stack (the CI roles need access to it)."
  type        = string
}

variable "github_oidc_subject" {
  description = <<-EOT
    Subject prefix of this repository's GitHub OIDC tokens: the only repository whose
    workflows may assume the CI roles. The repository uses immutable subjects
    (owner@owner_id/repo@repo_id), so a deleted and re-created repository with the same
    name cannot assume them. Check it with:
    gh api repos/<owner>/<repo>/actions/oidc/customization/sub (sub_claim_prefix).
  EOT
  type        = string
  default     = "repo:advaithvemurisai@219215219/wikiwatch@1403818521"

  validation {
    condition     = can(regex("^repo:[^*:]+$", var.github_oidc_subject))
    error_message = "github_oidc_subject must be a repo:... prefix without wildcards or event suffix."
  }
}

variable "vercel_team_slug" {
  description = "Vercel team slug; the Vercel OIDC issuer and audience are derived from it."
  type        = string
}

variable "vercel_project_name" {
  description = "Vercel project whose production deployments may read the dashboard snapshots."
  type        = string
  default     = "wikiwatch"
}

variable "alert_email" {
  description = "Address for budget and Athena alerts."
  type        = string
  sensitive   = true

  # Fail at plan time, not halfway through an apply: SNS and budget actions reject a
  # malformed address only when the resource is created.
  validation {
    condition     = can(regex("^[^[:space:]@\"'<>]+@[^[:space:]@\"'<>]+\\.[A-Za-z]{2,}$", var.alert_email))
    error_message = "alert_email must be a single email address, with no spaces or quotes."
  }
}

variable "budget_alert_thresholds_usd" {
  description = "Monthly actual-spend alert thresholds in USD (docs/plan.md, Cost control)."
  type        = list(number)
  default     = [10, 25, 40]
}

variable "budget_limit_usd" {
  description = "Monthly budget: forecast alert and the budget action both trigger at this amount."
  type        = number
  default     = 50
}

variable "athena_daily_alarm_gb" {
  description = "Alarm when the workgroup scans more than this many GB in a UTC day."
  type        = number
  default     = 20
}
