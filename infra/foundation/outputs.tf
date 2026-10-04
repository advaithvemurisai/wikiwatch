# Read by the compute stack through terraform_remote_state. Nothing here is a secret.

output "lake_bucket" {
  description = "Lake bucket name."
  value       = aws_s3_bucket.lake.bucket
}

output "athena_workgroup" {
  description = "Athena workgroup for dbt and the snapshot export."
  value       = aws_athena_workgroup.wikiwatch.name
}

output "ec2_instance_profile" {
  description = "Instance profile for session instances."
  value       = aws_iam_instance_profile.ec2.name
}

output "github_plan_role_arn" {
  description = "Store as the AWS_PLAN_ROLE_ARN repo secret."
  value       = aws_iam_role.github_plan.arn
}

output "github_deploy_role_arn" {
  description = "Store as the AWS_DEPLOY_ROLE_ARN repo secret."
  value       = aws_iam_role.github_deploy.arn
}

output "vercel_role_arn" {
  description = "Set as AWS_ROLE_ARN in the Vercel project's production environment."
  value       = aws_iam_role.vercel.arn
}
