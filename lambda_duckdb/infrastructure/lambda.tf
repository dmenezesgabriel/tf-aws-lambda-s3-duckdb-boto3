locals {
  lambda_function_file_name = "lambda_function"
  function_name             = "duckdb_bench"
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "iam_lambda_${local.function_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role_policy_attachment" "s3_lambda_policy" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = var.lambda_policy_arn

}

resource "aws_iam_policy" "test_s3_bucket_access" {
  name = "policy_${local.function_name}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })

}

resource "aws_iam_role_policy_attachment" "s3_lambda_test_s3_bucket_access" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.test_s3_bucket_access.arn

}

data "archive_file" "lambda_function" {
  type        = "zip"
  source_file = "${abspath("${path.module}/..")}/${local.lambda_function_file_name}.py"
  output_path = "${abspath("${path.module}/..")}/${local.lambda_function_file_name}.zip"
}

resource "aws_lambda_function_url" "lambda_function" {
  function_name      = aws_lambda_function.lambda_function.function_name
  authorization_type = "NONE"
}

resource "aws_lambda_function" "lambda_function" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename         = "${abspath("${path.module}/..")}/${local.lambda_function_file_name}.zip"
  function_name    = local.function_name
  handler          = "lambda_function.lambda_handler"
  layers           = [aws_lambda_layer_version.lambda_layer.arn]
  role             = aws_iam_role.iam_for_lambda.arn
  runtime          = "python3.11"
  memory_size      = 128
  timeout          = 3
  source_code_hash = data.archive_file.lambda_function.output_base64sha256

  environment {
    variables = {
      foo = "bar"
    }
  }

  tracing_config {
    mode = "Active"
  }
}

# Cloudwatch logs
resource "aws_cloudwatch_log_group" "lambda_function" {
  name              = "/aws/lambda/${aws_lambda_function.lambda_function.function_name}"
  retention_in_days = 30
}
