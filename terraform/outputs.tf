output "raw_zone_bucket" {
  description = "S3 bucket for Raw Zone landing data"
  value       = aws_s3_bucket.raw_zone.bucket
}

output "silver_zone_bucket" {
  description = "S3 bucket for Silver Zone cleansed data"
  value       = aws_s3_bucket.silver_zone.bucket
}

output "gold_zone_bucket" {
  description = "S3 bucket for Gold Zone aggregated data"
  value       = aws_s3_bucket.gold_zone.bucket
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results and execution logs"
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database_name" {
  description = "AWS Glue Catalog Database Name"
  value       = aws_glue_catalog_database.analytics_db.name
}

output "lambda_raw_ingest_function_name" {
  description = "Name of the raw ingestion Lambda function"
  value       = aws_lambda_function.raw_ingest.function_name
}

output "lambda_raw_ingest_arn" {
  description = "ARN of the raw ingestion Lambda function"
  value       = aws_lambda_function.raw_ingest.arn
}

output "sns_alerts_topic_arn" {
  description = "ARN of the SNS topic for pipeline alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup for analytics"
  value       = aws_athena_workgroup.analytics_workgroup.name
}
