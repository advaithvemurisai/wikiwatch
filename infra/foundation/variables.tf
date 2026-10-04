variable "aws_region" {
  description = "Region for every WikiWatch resource."
  type        = string
  default     = "us-east-1"
}

variable "state_bucket" {
  description = "Terraform state bucket from the bootstrap stack (the CI roles need access to it)."
  type        = string
}

variable "github_repo" {
  description = "The only GitHub repository whose workflows may assume the CI roles."
  type        = string
  default     = "advaithvemurisai/wikiwatch"
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
