# Glue databases (Iceberg namespaces on AWS) and the Athena workgroup.

locals {
  glue_databases = ["ref", "bronze", "silver", "gold", "ops"]
  one_gb         = 1024 * 1024 * 1024
}

resource "aws_glue_catalog_database" "lake" {
  for_each = toset(local.glue_databases)

  name = each.key
}

resource "aws_athena_workgroup" "wikiwatch" {
  name          = "wikiwatch"
  force_destroy = false

  configuration {
    # Clients (dbt, the snapshot export) cannot override these settings.
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = local.one_gb
    # Needed for the daily scan alarm below.
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/${local.athena_results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Athena has no daily scan cap that Terraform can set, so the 20 GB/day limit from
# docs/plan.md is an alarm instead (ADR 0010). The 1 GB per-query cap above is hard.
resource "aws_sns_topic" "alerts" {
  name = "wikiwatch-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "athena_daily_scan" {
  alarm_name          = "wikiwatch-athena-daily-scan"
  alarm_description   = "Athena workgroup scanned more than ${var.athena_daily_alarm_gb} GB in one day."
  namespace           = "AWS/Athena"
  metric_name         = "ProcessedBytes"
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.athena_daily_alarm_gb * local.one_gb
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    WorkGroup = aws_athena_workgroup.wikiwatch.name
  }
}
