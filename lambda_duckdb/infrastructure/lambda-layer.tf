locals {
  layer_path        = "lambda_layer"
  layer_zip_name    = "layer.zip"
  layer_name        = "lambda_layer_duckdb"
  requirements_name = "requirements.txt"
  requirements_path = "${abspath("${path.module}/..")}/${local.requirements_name}"
}

resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }
  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = <<EOT
      cd ${local.layer_path}
      rm -rf python
      mkdir python
      pip3 install -r ${local.requirements_path} -t python/
      zip -r ${local.layer_zip_name} python/
    EOT
  }
}

# define existing bucket for storing lambda layers
resource "aws_s3_bucket" "lambda_layer" {
  bucket = "lambda-layer-duckdb"
}

# upload zip file to s3
resource "aws_s3_object" "lambda_layer_zip" {
  bucket     = aws_s3_bucket.lambda_layer.id
  key        = "lambda_layers/${local.layer_name}/${local.layer_zip_name}"
  source     = "${local.layer_path}/${local.layer_zip_name}"
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

# create lambda layer from s3 object
resource "aws_lambda_layer_version" "lambda_layer" {
  s3_bucket           = aws_s3_bucket.lambda_layer.id
  s3_key              = aws_s3_object.lambda_layer_zip.key
  layer_name          = local.layer_name
  compatible_runtimes = ["python3.11"]
  depends_on          = [aws_s3_object.lambda_layer_zip] # triggered only if the zip file is uploaded to the bucket
}
