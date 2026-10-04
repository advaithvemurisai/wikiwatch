# Monthly cost budget: email alerts at $10, $25 and $40 actual and $50 forecast, plus an
# automatic action at $50 actual that stops anything new from launching.
#
# AWS's built-in "stop instances" budget action needs fixed instance IDs, and session
# instances get new IDs every time, so running instances are covered instead by the
# 4-hour self-shutdown and nightly-destroy (ADR 0010).

resource "aws_budgets_budget" "monthly" {
  name         = "wikiwatch-monthly"
  budget_type  = "COST"
  limit_amount = format("%.1f", var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Credits would otherwise net the cost to zero and no alert would ever fire.
  cost_types {
    include_credit = false
    include_refund = false
  }

  dynamic "notification" {
    for_each = var.budget_alert_thresholds_usd

    content {
      notification_type          = "ACTUAL"
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      subscriber_email_addresses = [var.alert_email]
    }
  }

  notification {
    notification_type          = "FORECASTED"
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_limit_usd
    threshold_type             = "ABSOLUTE_VALUE"
    subscriber_email_addresses = [var.alert_email]
  }
}

data "aws_iam_policy_document" "budget_stop" {
  statement {
    sid       = "NothingNewLaunches"
    effect    = "Deny"
    actions   = ["ec2:RunInstances", "rds:CreateDBInstance"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "budget_stop" {
  name        = "wikiwatch-budget-stop"
  description = "Attached by the budget action at the monthly limit: blocks new instances."
  policy      = data.aws_iam_policy_document.budget_stop.json
}

# The role AWS Budgets assumes to attach the deny policy, and nothing else.
data "aws_iam_policy_document" "budget_action_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "budget_action" {
  name               = "wikiwatch-budget-action"
  description        = "Used by AWS Budgets to attach the stop policy."
  assume_role_policy = data.aws_iam_policy_document.budget_action_trust.json
}

data "aws_iam_policy_document" "budget_action" {
  statement {
    actions   = ["iam:AttachRolePolicy", "iam:DetachRolePolicy"]
    resources = [aws_iam_role.github_deploy.arn]

    condition {
      test     = "ArnEquals"
      variable = "iam:PolicyARN"
      values   = [aws_iam_policy.budget_stop.arn]
    }
  }
}

resource "aws_iam_role_policy" "budget_action" {
  name   = "wikiwatch-budget-action"
  role   = aws_iam_role.budget_action.id
  policy = data.aws_iam_policy_document.budget_action.json
}

resource "aws_budgets_budget_action" "stop" {
  budget_name        = aws_budgets_budget.monthly.name
  action_type        = "APPLY_IAM_POLICY"
  approval_model     = "AUTOMATIC"
  notification_type  = "ACTUAL"
  execution_role_arn = aws_iam_role.budget_action.arn

  action_threshold {
    action_threshold_type  = "ABSOLUTE_VALUE"
    action_threshold_value = var.budget_limit_usd
  }

  # Every launch in this project goes through the deploy role (demo-up).
  definition {
    iam_action_definition {
      policy_arn = aws_iam_policy.budget_stop.arn
      roles      = [aws_iam_role.github_deploy.name]
    }
  }

  subscriber {
    address           = var.alert_email
    subscription_type = "EMAIL"
  }

  depends_on = [aws_iam_role_policy.budget_action]
}
