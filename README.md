# AWS Data Engineering Lakehouse ETL Platform

Production-grade AWS Lakehouse Data Engineering platform implementing the **Medallion Architecture** (Raw $\rightarrow$ Silver $\rightarrow$ Gold) using **Apache Airflow**, **Amazon S3**, **AWS Glue**, **PySpark**, **Amazon Athena**, **AWS Lambda**, **Amazon SNS**, and **Terraform** Infrastructure-as-Code.

---

## 🚀 Key Architectural Highlights

- **Medallion Lakehouse Pattern**:
  - **Raw Zone (Landing)**: Immutable landing tier storing raw JSON transaction batches partitioned by ingestion date (`year=YYYY/month=MM/day=DD/`).
  - **Silver Zone (Cleansed)**: Conformed, schema-enforced, and deduplicated Snappy-compressed Apache Parquet dataset partitioned by `tx_date=YYYY-MM-DD`.
  - **Gold Zone (Curated Marts)**: High-performance dimensional aggregations:
    1. `gold_daily_store_sales`: Daily store-level revenue, order volume, and 7-day rolling performance metrics (partitioned by `tx_date`).
    2. `gold_customer_metrics`: Customer 360 RFM (Recency, Frequency, Monetary) analytics with full native `DATE` typing.
- **Dual-Mode Execution Engine**:
  - **Direct Engine**: Airflow-native execution powered by `pandas`, `pyarrow`, and `boto3` for fast, lightweight, and serverless data processing directly against S3.
  - **AWS Glue Engine**: Serverless distributed PySpark 3.4 Glue ETL jobs with `--extra-py-files` packaging.
- **Apache Airflow Orchestration**:
  - Full containerized LocalExecutor environment running in Docker Compose.
  - S3KeySensors, idempotent partition overwrite logic, automated Athena catalog partition repairs, and automated Amazon SNS notifications.
- **Athena SQL Analytics & Automated Audit**:
  - Pre-packaged analytical models (Store Revenue Rankings, Customer RFM Segmentation cohorts) and an automated **Data Quality Reconciliation Audit** verifying zero revenue delta across Silver and Gold tiers.
- **Infrastructure as Code (Terraform)**:
  - Declarative provisioning of 5 encrypted S3 buckets (SSE-AES256, public access blocks, Glacier lifecycle transitions), Glue Data Catalog, Lambda function, Athena Workgroup, and SNS alerting topic.
- **Automated Testing & CI/CD**:
  - 100% pass rate across 14 PyTest unit tests (PySpark ETL logic, Boto3 AWS helpers, Lambda ingestion, and DAG structural validation).
  - GitHub Actions CI/CD automation workflow (`.github/workflows/ci.yml`).

---

## 📁 Repository Structure

```
DE Proj AWS/
├── .github/
│   └── workflows/
│       └── ci.yml                        # GitHub Actions CI workflow (linting & pytest)
├── dags/
│   └── aws_de_pipeline_dag.py            # Airflow DAG for end-to-end Lakehouse orchestration
├── data/
│   └── sample_raw_transactions.json      # Seed raw transaction dataset
├── docs/
│   └── architecture.md                   # Complete architectural design and data dictionary
├── scripts/
│   ├── run_athena_queries.py             # Automates Athena DDL, MSCK repairs & analytical queries
│   ├── start_airflow.ps1                 # Launches Docker Desktop and spins up Airflow containers
│   ├── stop_airflow.ps1                  # Stops local Airflow Docker containers
│   └── upload_artifacts.py               # Packages utils zip and uploads Glue scripts to S3
├── sql/
│   ├── ddl/
│   │   ├── 01_create_silver_transactions.sql
│   │   ├── 02_create_gold_daily_store_sales.sql
│   │   └── 03_create_gold_customer_metrics.sql
│   └── analytics/
│       ├── 01_daily_store_sales_performance.sql
│       ├── 02_customer_rfm_segmentation.sql
│       └── 03_data_quality_reconciliation.sql
├── src/
│   ├── ingestion/
│   │   └── lambda_raw_ingest.py          # AWS Lambda raw ingestion handler
│   ├── etl/
│   │   ├── direct_lakehouse_etl.py       # Direct Lakehouse ETL engine (Pandas/PyArrow/Boto3)
│   │   ├── glue_bronze_to_silver.py      # PySpark Glue job (Raw -> Silver Parquet)
│   │   └── glue_silver_to_gold.py        # PySpark Glue job (Silver -> Gold Aggregations)
│   ├── utils/
│   │   ├── aws_helpers.py                # Boto3 wrappers (Glue Crawler, SNS Alerts)
│   │   └── spark_utils.py                # PySpark session, DQ, argument parser helpers
│   └── local_pipeline_runner.py          # Standalone local pipeline runner
├── terraform/
│   ├── main.tf                           # Complete IaC (S3, IAM, Lambda, Glue, Athena, SNS)
│   ├── variables.tf                      # Environment & deployment variables
│   ├── outputs.tf                        # Resource ARNs, bucket names & workgroup
│   └── terraform.tfvars.example          # Example configuration parameters
├── tests/
│   ├── test_aws_helpers.py               # Unit tests for Boto3 Crawler and SNS helpers
│   ├── test_dag_validation.py            # Airflow DAG structural integrity tests
│   ├── test_lambda_ingestion.py          # AWS Lambda ingestion tests
│   └── test_spark_etl.py                 # PySpark transformation and DQ unit tests
├── .env.example                          # Example environment configuration for Airflow
├── docker-compose.yml                    # Apache Airflow LocalExecutor Docker Compose
├── pyproject.toml                        # Project metadata and tool settings
├── requirements.txt                      # Python dependencies
└── README.md
```

---

## 🛠️ Quickstart & Local Setup

### 1. Python Environment Setup
```powershell
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install project dependencies
pip install -r requirements.txt
```

### 2. Configure AWS CLI
Ensure your AWS credentials are configured:
```powershell
aws configure
# AWS Access Key ID: <YOUR_KEY>
# AWS Secret Access Key: <YOUR_SECRET>
# Default region name: ap-south-1
# Default output format: json
```

### 3. Deploy Cloud Infrastructure (Terraform)
```powershell
cd terraform
terraform init
terraform plan
terraform apply
cd ..
```

---

## 🐳 Running Apache Airflow Locally

Airflow is containerized with Docker Compose using `LocalExecutor` and includes `apache-airflow-providers-amazon`.

### Start Airflow
```powershell
powershell scripts/start_airflow.ps1
```
*The script checks Docker Desktop, waits for the engine to initialize, and brings up Airflow.*

- **Web UI URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `admin`
- **Password**: `admin`
- **DAG**: `aws_de_lakehouse_etl_pipeline`

### Trigger the Pipeline
Trigger the DAG from the Web UI or via CLI:
```powershell
docker exec airflow-webserver airflow dags trigger aws_de_lakehouse_etl_pipeline
```

### Stop Airflow
```powershell
powershell scripts/stop_airflow.ps1
```

---

## 📊 End-to-End Execution Flow

```
[S3 Raw Zone] ──(S3KeySensor)──> [Bronze -> Silver] ──> [Silver -> Gold] ──> [Athena MSCK Sync] ──> [SNS Alert]
```

1. **Wait for Raw Data (`wait_for_raw_s3_data`)**:
   `S3KeySensor` polls `s3://<raw-bucket>/raw/transactions/year=YYYY/month=MM/day=DD/` for inbound JSON.
2. **Bronze to Silver (`run_bronze_to_silver_transformation`)**:
   Validates schema, filters null keys, standardizes payment methods, calculates missing amounts, deduplicates by `transaction_id`, appends audit metadata (`ingested_at`, `data_source`), and writes partitioned Parquet to Silver.
3. **Silver to Gold (`run_silver_to_gold_aggregation`)**:
   - Generates daily store revenue summaries (`gold_daily_store_sales`).
   - Generates Customer 360 RFM loyalty metrics (`gold_customer_metrics`).
4. **Athena Catalog Repair (`sync_athena_partitions`)**:
   Executes `MSCK REPAIR TABLE` via Amazon Athena to instantly register newly created S3 partitions in the Glue Data Catalog.
5. **SNS Alerting (`send_pipeline_success_notification`)**:
   Publishes pipeline execution status, timestamp, and metadata to Amazon SNS.

---

## 📈 Athena Analytics & Reconciliation Queries

Execute the automated Athena query suite against live S3 Lakehouse data:
```powershell
python scripts/run_athena_queries.py
```

### Sample Output: Daily Store Sales Performance
| Store ID | Transaction Date | Revenue Rank | Total Revenue (USD) | Total Tx | Unique Customers | Avg Order Value | Total Units Sold |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `STORE_NY_01` | `2026-09-25` | **1** | **$219.98** | 2 | 2 | $109.99 | 3 |
| `STORE_CA_02` | `2026-09-25` | **2** | **$75.00** | 1 | 1 | $75.00 | 5 |

### Sample Output: Customer 360 RFM Segmentation
| Customer ID | Total Orders | Lifetime Spend (USD) | Avg Basket Value | Days Since Order | Customer Segment |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `CUST_501` | 2 | **$174.98** | $87.49 | 6 | `CHAMPION` / `AT_RISK_HIGH_VALUE` |
| `CUST_502` | 1 | **$120.00** | $120.00 | 6 | `STANDARD_ACTIVE` / `LOYAL_CUSTOMER` |

### Sample Output: Reconciliation Audit
| Audit Date | Silver Revenue | Gold Revenue | Revenue Delta | Status |
| :---: | :---: | :---: | :---: | :---: |
| **`2026-09-25`** | **$589.96** | **$589.96** | **$0.00** | **`PASSED`** |

---

## 🧪 Testing

Execute the complete test suite:
```powershell
venv\Scripts\pytest tests/ -v
```

All 14 unit tests pass, validating:
- Glue Crawler trigger & status polling logic.
- SNS notification dispatch & error handling.
- Airflow DAG structure, cycles, and task dependencies.
- Lambda raw payload parsing, validation, and S3 batch writes.
- PySpark data quality checks, schema conversions, and Gold aggregations.

---

## 📄 License
This project is licensed under the Apache 2.0 License.
