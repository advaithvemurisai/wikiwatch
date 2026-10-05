# IAM: the EC2 instance role, two GitHub OIDC roles (read-only plan for PRs, deploy for
# main) and the Vercel OIDC role. No IAM user and no access key exists anywhere.

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  arn_prefix = "arn:${local.partition}"

  glue_catalog_arn   = "${local.arn_prefix}:glue:${var.aws_region}:${local.account_id}:catalog"
  glue_database_arns = [for db in local.glue_databases : "${local.arn_prefix}:glue:${var.aws_region}:${local.account_id}:database/${db}"]
  glue_table_arns    = [for db in local.glue_databases : "${local.arn_prefix}:glue:${var.aws_region}:${local.account_id}:table/${db}/*"]
  state_bucket_arn   = "${local.arn_prefix}:s3:::${var.state_bucket}"
  ssm_param_arn      = "${local.arn_prefix}:ssm:${var.aws_region}:${local.account_id}:parameter/wikiwatch"
  # Public parameter with the current Ubuntu AMI ID (read by the compute stack).
  ssm_public_ami_arn = "${local.arn_prefix}:ssm:${var.aws_region}::parameter/aws/service/canonical/*"

  github_oidc_host = "token.actions.githubusercontent.com"
  vercel_oidc_host = "oidc.vercel.com/${var.vercel_team_slug}"
}

# ------------------------------------------------------------------ EC2 instance role

data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  name               = "wikiwatch-ec2"
  description        = "WikiWatch session instance: lake bucket, Glue, Athena, /wikiwatch/* parameters, SSM sessions."
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

data "aws_iam_policy_document" "ec2" {
  statement {
    sid       = "LakeBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  # Spark's Glue catalog and dbt-athena. Databases come from Terraform, so no
  # CreateDatabase or DeleteDatabase.
  statement {
    sid = "GlueTables"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:GetTableVersions",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = concat([local.glue_catalog_arn], local.glue_database_arns, local.glue_table_arns)
  }

  statement {
    sid = "AthenaWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.wikiwatch.arn]
  }

  # Session secrets, written by hand under /wikiwatch/ (never by Terraform). SecureStrings
  # use the AWS-managed key, whose key policy already allows use through SSM.
  statement {
    sid       = "SessionParameters"
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = [local.ssm_param_arn, "${local.ssm_param_arn}/*"]
  }

  # Session Manager only. The managed AmazonSSMManagedInstanceCore policy would also grant
  # ssm:GetParameter on every parameter in the account, so the agent's needs are listed here.
  statement {
    sid = "SessionManager"
    actions = [
      "ssm:UpdateInstanceInformation",
      "ssm:ListInstanceAssociations",
      "ssm:DescribeAssociation",
      "ssm:GetDocument",
      "ssm:DescribeDocument",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ec2" {
  name   = "wikiwatch-ec2"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2.json
}

resource "aws_iam_instance_profile" "ec2" {
  name = "wikiwatch-ec2"
  role = aws_iam_role.ec2.name
}

# ------------------------------------------------------------------ GitHub OIDC

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://${local.github_oidc_host}"
  client_id_list = ["sts.amazonaws.com"]
}

# One trust policy per role; `subject` pins the repository and the event or branch.
data "aws_iam_policy_document" "github_trust" {
  for_each = {
    plan   = "${var.github_oidc_subject}:pull_request"
    deploy = "${var.github_oidc_subject}:ref:refs/heads/main"
  }

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_oidc_host}:sub"
      values   = [each.value]
    }
  }
}

# Read-only role for `terraform plan` on pull requests. It can read resource
# configuration and the state files, but not lake data, and cannot take the state lock
# (PR plans run with -lock=false).
resource "aws_iam_role" "github_plan" {
  name                 = "wikiwatch-github-plan"
  description          = "terraform plan on pull requests (read-only)."
  assume_role_policy   = data.aws_iam_policy_document.github_trust["plan"].json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "github_plan" {
  statement {
    sid       = "StateRead"
    actions   = ["s3:GetObject"]
    resources = ["${local.state_bucket_arn}/*"]
  }

  # Bucket-level reads only (these ARNs are buckets, not objects), so lake data is unreadable.
  statement {
    sid       = "BucketConfigRead"
    actions   = ["s3:Get*", "s3:List*"]
    resources = [local.state_bucket_arn, aws_s3_bucket.lake.arn]
  }

  statement {
    sid = "ConfigRead"
    actions = [
      "ec2:Describe*",
      # IAM reads include tags and policy versions; no IAM read returns a secret value.
      "iam:Get*",
      "iam:List*",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTags",
      "athena:GetWorkGroup",
      "athena:ListTagsForResource",
      "sns:GetTopicAttributes",
      "sns:GetSubscriptionAttributes",
      "sns:ListTagsForResource",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:ListTagsForResource",
      "budgets:ViewBudget",
      "budgets:DescribeBudgetAction",
      "budgets:DescribeBudgetActionsForBudget",
      "budgets:ListTagsForResource",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "AmiLookup"
    actions   = ["ssm:GetParameter"]
    resources = [local.ssm_public_ami_arn]
  }
}

resource "aws_iam_role_policy" "github_plan" {
  name   = "wikiwatch-github-plan"
  role   = aws_iam_role.github_plan.id
  policy = data.aws_iam_policy_document.github_plan.json
}

# Deploy role for demo-up, demo-down and nightly-destroy, from main only. It can manage
# the compute stack and nothing else: foundation and bootstrap are applied by hand.
resource "aws_iam_role" "github_deploy" {
  name                 = "wikiwatch-github-deploy"
  description          = "Compute stack apply and destroy from main."
  assume_role_policy   = data.aws_iam_policy_document.github_trust["deploy"].json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "StateBucket"
    actions   = ["s3:ListBucket"]
    resources = [local.state_bucket_arn]
  }

  statement {
    sid       = "ComputeState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.state_bucket_arn}/compute/*"]
  }

  # The compute stack reads foundation outputs (lake bucket, instance profile).
  statement {
    sid       = "FoundationStateRead"
    actions   = ["s3:GetObject"]
    resources = ["${local.state_bucket_arn}/foundation/*"]
  }

  statement {
    sid = "ComputeNetworkAndInstance"
    actions = [
      "ec2:Describe*",
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:CreateVpc",
      "ec2:DeleteVpc",
      "ec2:ModifyVpcAttribute",
      "ec2:CreateSubnet",
      "ec2:DeleteSubnet",
      "ec2:ModifySubnetAttribute",
      "ec2:CreateInternetGateway",
      "ec2:DeleteInternetGateway",
      "ec2:AttachInternetGateway",
      "ec2:DetachInternetGateway",
      "ec2:CreateRouteTable",
      "ec2:DeleteRouteTable",
      "ec2:CreateRoute",
      "ec2:DeleteRoute",
      "ec2:AssociateRouteTable",
      "ec2:DisassociateRouteTable",
      "ec2:CreateVpcEndpoint",
      "ec2:DeleteVpcEndpoints",
      "ec2:ModifyVpcEndpoint",
      "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:RunInstances",
      "ec2:TerminateInstances",
      "ec2:ModifyInstanceAttribute",
      "ec2:ModifyInstanceMetadataOptions",
      "ec2:ModifyInstanceCreditSpecification",
      "ec2:CancelSpotInstanceRequests",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  # The only instance type the deploy role can launch.
  statement {
    sid       = "OnlyTheSessionInstanceType"
    effect    = "Deny"
    actions   = ["ec2:RunInstances"]
    resources = ["${local.arn_prefix}:ec2:*:*:instance/*"]

    condition {
      test     = "StringNotEquals"
      variable = "ec2:InstanceType"
      values   = ["t4g.xlarge"]
    }
  }

  statement {
    sid       = "PassOnlyTheInstanceRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.ec2.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }

  statement {
    sid       = "InstanceProfileRead"
    actions   = ["iam:GetInstanceProfile", "iam:GetRole"]
    resources = [aws_iam_instance_profile.ec2.arn, aws_iam_role.ec2.arn]
  }

  # The first spot request in an account creates the EC2 Spot service-linked role.
  statement {
    sid       = "SpotServiceLinkedRole"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["spot.amazonaws.com"]
    }
  }

  statement {
    sid       = "AmiLookup"
    actions   = ["ssm:GetParameter"]
    resources = [local.ssm_public_ami_arn]
  }

  # Soak flag: demo-up sets it, demo-down clears it, nightly-destroy reads it.
  statement {
    sid       = "SoakFlag"
    actions   = ["ssm:GetParameter", "ssm:PutParameter", "ssm:DeleteParameter"]
    resources = ["${local.ssm_param_arn}/soak_until"]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "wikiwatch-github-deploy"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}

# ------------------------------------------------------------------ Vercel OIDC

# Team issuer mode: tokens come from oidc.vercel.com/<team> with audience
# https://vercel.com/<team>.
resource "aws_iam_openid_connect_provider" "vercel" {
  url            = "https://${local.vercel_oidc_host}"
  client_id_list = ["https://vercel.com/${var.vercel_team_slug}"]
}

data "aws_iam_policy_document" "vercel_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.vercel.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.vercel_oidc_host}:aud"
      values   = ["https://vercel.com/${var.vercel_team_slug}"]
    }

    # Production only: preview deployments use fixtures and get no AWS access.
    condition {
      test     = "StringEquals"
      variable = "${local.vercel_oidc_host}:sub"
      values   = ["owner:${var.vercel_team_slug}:project:${var.vercel_project_name}:environment:production"]
    }
  }
}

resource "aws_iam_role" "vercel" {
  name                 = "wikiwatch-vercel-dashboard"
  description          = "Vercel production: read dashboard snapshots only."
  assume_role_policy   = data.aws_iam_policy_document.vercel_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "vercel" {
  statement {
    sid       = "DashboardSnapshots"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/${local.dashboard_prefix}*"]
  }
}

resource "aws_iam_role_policy" "vercel" {
  name   = "wikiwatch-vercel-dashboard"
  role   = aws_iam_role.vercel.id
  policy = data.aws_iam_policy_document.vercel.json
}
