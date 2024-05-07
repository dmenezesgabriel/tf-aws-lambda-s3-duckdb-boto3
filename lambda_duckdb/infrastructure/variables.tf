variable "environment" {
  description = "Project environment"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "lambda_policy_arn" {
  description = "ARN of the IAM role policy to attach to the lambda role."
  type        = string
  default     = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
