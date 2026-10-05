# Foundation: everything that outlives a session (lake bucket, catalog, Athena, IAM,
# budget). Applied by hand from CloudShell (docs/first-apply-checklist.md); CI only plans it.

terraform {
  required_version = "1.16.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.67.0"
    }
  }

  backend "s3" {
    key          = "foundation/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = "wiki-lakehouse"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
