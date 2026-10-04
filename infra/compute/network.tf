# One public subnet, no NAT gateway, no Elastic IP. The instance gets an ephemeral public
# IPv4 address for outbound traffic only (Wikimedia, image pulls, SSM); S3 traffic stays on
# the free gateway endpoint.

locals {
  vpc_cidr    = "10.42.0.0/16"
  subnet_cidr = "10.42.1.0/24"
  any_ipv4    = "0.0.0.0/0"
}

# Pick an availability zone that actually offers the instance type.
data "aws_ec2_instance_type_offerings" "session" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [local.instance_type]
  }
}

resource "aws_vpc" "session" {
  cidr_block           = local.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "wikiwatch-session"
  }
}

resource "aws_internet_gateway" "session" {
  vpc_id = aws_vpc.session.id
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.session.id
  cidr_block              = local.subnet_cidr
  availability_zone       = sort(data.aws_ec2_instance_type_offerings.session.locations)[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "wikiwatch-session-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.session.id

  route {
    cidr_block = local.any_ipv4
    gateway_id = aws_internet_gateway.session.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.session.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
}

# No ingress rules at all: every UI is reached through SSM port forwarding.
resource "aws_security_group" "session" {
  name        = "wikiwatch-session"
  description = "No inbound; outbound for Wikimedia, image pulls, SSM and AWS APIs."
  vpc_id      = aws_vpc.session.id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.session.id
  description       = "All outbound"
  ip_protocol       = "-1"
  cidr_ipv4         = local.any_ipv4
}
