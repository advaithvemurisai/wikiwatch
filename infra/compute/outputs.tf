# No IP address or hostname is ever output. Connect with:
#   aws ssm start-session --target <instance id>

output "instance_id" {
  description = "Session instance ID, for SSM Session Manager."
  value       = aws_instance.session.id
}
