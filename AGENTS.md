# Antigravity Workspace Guidelines & Persistent Memory

## Project State Summary
This project is an AWS Data Engineering Lakehouse pipeline configured in `ap-south-1`.

### Key Configurations & Scripts
- **AWS Region**: `ap-south-1`
- **AWS Account ID**: `939139585522` (`arn:aws:iam::939139585522:user/de-admin`)
- **Terraform Config**: Configured in `terraform/terraform.tfvars`.
- **Upload Artifacts**: Run `python scripts/upload_artifacts.py` to package & upload Glue utils to S3.
- **Run Athena Queries & Data Sync**: Run `python scripts/run_athena_queries.py`.
- **Run Unit Tests**: Run `venv\Scripts\pytest tests/ -v`.
- **Local Airflow (Docker)**: Run `powershell scripts/start_airflow.ps1` to launch Airflow; Web UI at `http://localhost:8080` (admin/admin). Stop with `powershell scripts/stop_airflow.ps1`.
- **CI/CD Workflow**: Located at `.github/workflows/ci.yml`.
- **Latest Handover Summary**: See `docs/session_summary_2026-10-14.md` for full log.
- **Glue Infrastructure Status**:
  - Silver & Gold Glue Crawlers (`de-proj-aws-silver-crawler-dev`, `de-proj-aws-gold-crawler-dev`) and PySpark Jobs (`de-proj-aws-bronze-to-silver`, `de-proj-aws-silver-to-gold`) deployed via Terraform.
  - Lake Formation permissions configured with `IAM_ALLOWED_PRINCIPALS` default permissions.
  - End-to-end Airflow DAG (`aws_de_lakehouse_etl_pipeline`) fully verified with `EXECUTION_ENGINE="glue"`.
- **Current Git Status**:
  - Active branch: `aws-de-pipeline` (tracking `origin/aws-de-pipeline`).
  - Latest commit: `8956252` pushed to GitHub.
