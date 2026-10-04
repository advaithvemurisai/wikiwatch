# The session instance: Graviton (same ARM64 images as the M1 Mac), no SSH key, no open
# port, IMDSv2 only, encrypted disk deleted with the instance.

locals {
  instance_type = "t4g.xlarge"
  foundation    = data.terraform_remote_state.foundation.outputs

  # Session limit: 4 hours, or 48 hours for the soak test (when the soak flag also expires).
  shutdown_minutes = var.soak_mode ? 48 * 60 : 4 * 60
}

data "aws_ssm_parameter" "ubuntu_arm64" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
}

resource "aws_instance" "session" {
  ami                    = data.aws_ssm_parameter.ubuntu_arm64.insecure_value
  instance_type          = local.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.session.id]
  iam_instance_profile   = local.foundation.ec2_instance_profile
  monitoring             = false

  # The self-shutdown terminates the instance, so a forgotten session leaves no running
  # instance and no disk behind.
  instance_initiated_shutdown_behavior = "terminate"

  dynamic "instance_market_options" {
    for_each = var.market == "spot" ? [1] : []

    content {
      market_type = "spot"

      spot_options {
        spot_instance_type             = "one-time"
        instance_interruption_behavior = "terminate"
      }
    }
  }

  # Standard credits: a busy session slows down instead of buying surplus CPU credits.
  credit_specification {
    cpu_credits = "standard"
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
    # 2 hops so containers on Docker's bridge network can reach the instance role.
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/user_data.sh.tftpl", {
    aws_region          = var.aws_region
    github_repo         = var.github_repo
    repo_tag            = var.repo_tag
    lake_bucket         = local.foundation.lake_bucket
    athena_workgroup    = local.foundation.athena_workgroup
    shutdown_minutes    = local.shutdown_minutes
    spark_mem_limit     = var.spark_mem_limit
    spark_driver_memory = var.spark_driver_memory
  })
  user_data_replace_on_change = true

  tags = {
    Name = "wikiwatch-session"
  }

  lifecycle {
    precondition {
      condition     = var.repo_tag != ""
      error_message = "Set repo_tag (the git tag to deploy) to create a session."
    }
  }
}
