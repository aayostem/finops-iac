variable "environment" { type = string }
resource "aws_resourcegroups_group" "finops" {
  name = "finops-${var.environment}"
  resource_query {
    query = jsonencode({
      ResourceTypeFilters = ["AWS::AllSupported"],
      TagFilters = [{ Key = "Environment", Values = [var.environment] }]
    })
  }
}
