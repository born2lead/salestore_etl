# AWS Data Engineering Pipeline Project Memory & State

## Overview
This repository contains an end-to-end AWS Data Engineering Pipeline built with PySpark, Terraform, AWS Lambda, AWS Glue, Amazon Athena, and GitHub Actions.

## AWS Credentials & Environment
- **Account ID**: `939139585522`
- **IAM User**: `arn:aws:iam::939139585522:user/de-admin`
- **Region**: `ap-south-1` (Mumbai)
- **Default Output**: `json`

## Deployed AWS Resources (Terraform)
- **S3 Lakehouse Buckets**:
  - `de-proj-aws-raw-zone-dev`
  - `de-proj-aws-silver-zone-dev`
  - `de-proj-aws-gold-zone-dev`
  - `de-proj-aws-scripts-dev`
  - `de-proj-aws-athena-results-dev`
- **Glue Catalog Database**: `de_proj_aws_db_dev`
- **Lambda Function**: `de-proj-aws-raw-ingest-dev`
- **Athena Workgroup**: `de-proj-aws-workgroup-dev`
- **Athena Named Queries**:
  - `Daily_Store_Sales_Performance`
  - `Customer_360_RFM_Segmentation`
  - `Data_Quality_Reconciliation_Audit`

## Automated Execution Scripts
1. **Artifact Build & Upload**: `python scripts/upload_artifacts.py`
   - Bundles `src/utils/` into `dist/pipeline_utils.zip`.
   - Uploads zip bundle and Glue ETL scripts to `s3://de-proj-aws-scripts-dev/`.

2. **Athena Query Execution & Data Sync**: `python scripts/run_athena_queries.py`
   - Syncs local Parquet files (`data/silver/`, `data/gold/`) to S3.
   - Registers external DDL tables in `de_proj_aws_db_dev`.
   - Runs `MSCK REPAIR TABLE` to load partitions.
   - Executes all 3 analytical SQL queries and prints formatted results.

3. **Local PyTest Suite**: `venv\Scripts\pytest tests/ -v`
   - 14/14 unit tests passing.

4. **CI/CD Pipeline**: `.github/workflows/ci.yml`
   - Automated PyTest, Flake8, Black, Terraform validation, and S3 artifact deployment on git push.
