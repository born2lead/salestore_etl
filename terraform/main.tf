terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# -----------------------------------------------------------------------------
# 1. S3 Lakehouse Bucket Tiers (Raw, Silver, Gold, Scripts, Athena Results)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "raw_zone" {
  bucket        = "${var.project_name}-raw-zone-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "silver_zone" {
  bucket        = "${var.project_name}-silver-zone-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "gold_zone" {
  bucket        = "${var.project_name}-gold-zone-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "scripts_bucket" {
  bucket        = "${var.project_name}-scripts-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-athena-results-${var.environment}"
  force_destroy = true
}

# -----------------------------------------------------------------------------
# 2. S3 Lifecycle Rules & Security Configurations
# -----------------------------------------------------------------------------
# Raw Zone Lifecycle: Transition to Glacier after 90 days, expire after 365 days
resource "aws_s3_bucket_lifecycle_configuration" "raw_zone_lifecycle" {
  bucket = aws_s3_bucket.raw_zone.id

  rule {
    id     = "archive-raw-to-glacier"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Server-Side Encryption Configuration for all project buckets
resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_encryption" {
  for_each = {
    raw     = aws_s3_bucket.raw_zone.id
    silver  = aws_s3_bucket.silver_zone.id
    gold    = aws_s3_bucket.gold_zone.id
    scripts = aws_s3_bucket.scripts_bucket.id
    athena  = aws_s3_bucket.athena_results.id
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Public Access Blocks for Lakehouse buckets
resource "aws_s3_bucket_public_access_block" "public_access_block" {
  for_each = {
    raw     = aws_s3_bucket.raw_zone.id
    silver  = aws_s3_bucket.silver_zone.id
    gold    = aws_s3_bucket.gold_zone.id
    scripts = aws_s3_bucket.scripts_bucket.id
    athena  = aws_s3_bucket.athena_results.id
  }

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# 3. AWS SNS Alerting Topic
# -----------------------------------------------------------------------------
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts-${var.environment}"
}

# -----------------------------------------------------------------------------
# 4. AWS Lambda Raw Ingestion Engine & IAM Role
# -----------------------------------------------------------------------------
data "archive_file" "lambda_ingest_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion/lambda_raw_ingest.py"
  output_path = "${path.module}/lambda_raw_ingest.zip"
}

resource "aws_iam_role" "lambda_ingest_role" {
  name = "${var.project_name}-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_attach" {
  role       = aws_iam_role.lambda_ingest_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "lambda_s3_put_policy" {
  name        = "${var.project_name}-lambda-s3-${var.environment}"
  description = "Allows Lambda to write raw ingested landing objects"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.raw_zone.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_ingest_role.name
  policy_arn = aws_iam_policy.lambda_s3_put_policy.arn
}

resource "aws_lambda_function" "raw_ingest" {
  function_name    = "${var.project_name}-raw-ingest-${var.environment}"
  role             = aws_iam_role.lambda_ingest_role.arn
  handler          = "lambda_raw_ingest.lambda_handler"
  runtime          = "python3.11"
  filename         = data.archive_file.lambda_ingest_zip.output_path
  source_code_hash = data.archive_file.lambda_ingest_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      RAW_BUCKET_NAME = aws_s3_bucket.raw_zone.bucket
    }
  }
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.raw_ingest.function_name}"
  retention_in_days = 14
}

# -----------------------------------------------------------------------------
# 5. AWS Glue Service IAM Role & Policies
# -----------------------------------------------------------------------------
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "s3_access_policy" {
  name        = "${var.project_name}-s3-policy-${var.environment}"
  description = "Allows full access to project S3 buckets for Glue jobs and crawlers"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_zone.arn,
          "${aws_s3_bucket.raw_zone.arn}/*",
          aws_s3_bucket.silver_zone.arn,
          "${aws_s3_bucket.silver_zone.arn}/*",
          aws_s3_bucket.gold_zone.arn,
          "${aws_s3_bucket.gold_zone.arn}/*",
          aws_s3_bucket.scripts_bucket.arn,
          "${aws_s3_bucket.scripts_bucket.arn}/*",
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# -----------------------------------------------------------------------------
# 6. AWS Glue Catalog Database & Crawlers (Silver & Gold)
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "analytics_db" {
  name        = "${replace(var.project_name, "-", "_")}_db_${var.environment}"
  description = "AWS Glue Data Catalog for Silver and Gold Zone Lakehouse tables"
}

resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.analytics_db.name
  name          = "${var.project_name}-silver-crawler-${var.environment}"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver_zone.bucket}/silver/transactions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.analytics_db.name
  name          = "${var.project_name}-gold-crawler-${var.environment}"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.gold_zone.bucket}/gold/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# -----------------------------------------------------------------------------
# 7. AWS Glue PySpark Jobs
# -----------------------------------------------------------------------------
resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project_name}-bronze-to-silver"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts_bucket.bucket}/src/etl/glue_bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--continuous-log-logGroup"         = "/aws-glue/jobs/${var.project_name}"
    "--enable-continuous-cloudwatch-log"= "true"
    "--extra-py-files"                  = "s3://${aws_s3_bucket.scripts_bucket.bucket}/dist/pipeline_utils.zip"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name         = "${var.project_name}-silver-to-gold"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts_bucket.bucket}/src/etl/glue_silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--continuous-log-logGroup"         = "/aws-glue/jobs/${var.project_name}"
    "--enable-continuous-cloudwatch-log"= "true"
    "--extra-py-files"                  = "s3://${aws_s3_bucket.scripts_bucket.bucket}/dist/pipeline_utils.zip"
  }
}

# -----------------------------------------------------------------------------
# 8. Athena Analytics Workgroup & Named Queries
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "analytics_workgroup" {
  name        = "${var.project_name}-workgroup-${var.environment}"
  description = "Dedicated Athena Workgroup for Lakehouse Analytics queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/output/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "daily_store_performance" {
  name      = "Daily_Store_Sales_Performance"
  workgroup = aws_athena_workgroup.analytics_workgroup.name
  database  = aws_glue_catalog_database.analytics_db.name
  description = "Identifies top performing stores, daily revenue, and rolling 7-day average"
  query     = file("${path.module}/../sql/analytics/01_daily_store_sales_performance.sql")
}

resource "aws_athena_named_query" "customer_rfm_segmentation" {
  name      = "Customer_360_RFM_Segmentation"
  workgroup = aws_athena_workgroup.analytics_workgroup.name
  database  = aws_glue_catalog_database.analytics_db.name
  description = "Classifies customer base into RFM loyalty tiers (Champions, Loyal, At Risk)"
  query     = file("${path.module}/../sql/analytics/02_customer_rfm_segmentation.sql")
}

resource "aws_athena_named_query" "data_quality_reconciliation" {
  name      = "Data_Quality_Reconciliation_Audit"
  workgroup = aws_athena_workgroup.analytics_workgroup.name
  database  = aws_glue_catalog_database.analytics_db.name
  description = "Reconciles transaction counts and revenue between Silver and Gold tiers"
  query     = file("${path.module}/../sql/analytics/03_data_quality_reconciliation.sql")
}
