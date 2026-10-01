# AWS Data Engineering Lakehouse Architecture

## 1. Executive Summary & Topology
This project implements an enterprise-grade AWS Lakehouse Data Engineering architecture following Medallion Architecture principles (Raw/Bronze -> Silver -> Gold). The platform ingests streaming or batch transaction payloads, normalizes and cleanses data at scale using serverless AWS Glue PySpark, models business-ready aggregations into the Gold zone, and exposes them to business intelligence tools and analysts via Amazon Athena and AWS Glue Data Catalog.

```
+---------------------------------------------------------------------------------------------------------+
|                                    AWS LAKEHOUSE ARCHITECTURE TOPOLOGY                                  |
+---------------------------------------------------------------------------------------------------------+

  [ External Data Source ]
  (Batch APIs / Webhooks)
             |
             v
  +----------------------+
  |   AWS Lambda         | ---> Writes raw immutable JSON batch
  |   (Ingest Engine)    |
  +----------------------+
             |
             v
  +-------------------------------------------------------------------------------------------------------+
  |  S3 RAW ZONE (Landing Tier)                                                                           |
  |  s3://<project>-raw-zone-<env>/raw/transactions/year=YYYY/month=MM/day=DD/                           |
  |  - Format: Raw UTF-8 JSON batch objects                                                               |
  |  - Retention: S3 Lifecycle -> Transition to Glacier after 90 days, Expire after 365 days              |
  +-------------------------------------------------------------------------------------------------------+
             |
             |  Triggered via Apache Airflow (MWAA)
             |  AWS Glue PySpark: Bronze -> Silver Job
             v
  +-------------------------------------------------------------------------------------------------------+
  |  S3 SILVER ZONE (Cleansed Tier)                                                                       |
  |  s3://<project>-silver-zone-<env>/silver/transactions/tx_date=YYYY-MM-DD/                             |
  |  - Format: Snappy-compressed Apache Parquet                                                           |
  |  - Logic: Schema enforcement, null filtering, deduplication on transaction_id, audit metadata tagging |
  +-------------------------------------------------------------------------------------------------------+
             |
             |  AWS Glue PySpark: Silver -> Gold Job
             v
  +-------------------------------------------------------------------------------------------------------+
  |  S3 GOLD ZONE (Business Aggregations Tier)                                                            |
  |  s3://<project>-gold-zone-<env>/gold/                                                                 |
  |  ├── daily_store_sales/ (Partitioned by tx_date=YYYY-MM-DD)                                           |
  |  └── customer_metrics/  (Customer 360 RFM Lifetime Summary Table)                                     |
  +-------------------------------------------------------------------------------------------------------+
             |
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
  +----------------------+                       +----------------------+
  | AWS Glue Crawlers    |                       | Amazon Athena        |
  | (Silver & Gold)      | ---- Registers -----> | Analytics Workgroup  |
  +----------------------+      Data Catalog     | & Presto SQL Queries |
                                                 +----------------------+
                                                             |
                                                             v
                                                 [ BI Dashboards / Reports ]
```

---

## 2. Lakehouse Storage Tiers & Data Dictionary

### 2.1 Raw Zone (`s3://<project>-raw-zone-<env>/`)
- **Purpose**: Immutable landing zone storing pristine raw transaction events.
- **Partition Scheme**: `year=YYYY/month=MM/day=DD/`
- **File Format**: JSON (`.json`)
- **Lifecycle & Governance**:
  - Transition to `S3 Glacier Flexible Retrieval` after 90 days.
  - Expiration after 365 days.
  - Abort incomplete multipart uploads after 7 days.
  - Server-Side Encryption (`AES256`).
  - S3 Public Access Block fully enabled.

### 2.2 Silver Zone (`s3://<project>-silver-zone-<env>/`)
- **Purpose**: Conformed, cleansed, deduplicated transaction records.
- **Partition Scheme**: `tx_date=YYYY-MM-DD`
- **File Format**: Snappy-compressed Apache Parquet.
- **Schema**:
  | Column Name | Data Type | Nullable | Description |
  |---|---|---|---|
  | `transaction_id` | STRING | No (PK) | Unique transaction business identifier (trimmed) |
  | `customer_id` | STRING | No | Unique customer identifier |
  | `store_id` | STRING | Yes | Store location ID |
  | `product_id` | STRING | Yes | Product SKU code |
  | `quantity` | INTEGER | No | Number of purchased items (defaulted to 1) |
  | `unit_price` | DOUBLE | Yes | Price per unit |
  | `total_amount` | DOUBLE | No | Final order amount (computed if missing) |
  | `payment_method` | STRING | No | Standardized payment mode (`CARD`, `CASH`, `UPI`, `UNKNOWN`) |
  | `tx_timestamp` | TIMESTAMP | No | Normalized transaction timestamp |
  | `calculated_amount` | DOUBLE | Yes | Derived metric: `quantity * unit_price` |
  | `ingested_at` | TIMESTAMP | No | Audit column: ETL processing timestamp |
  | `data_source` | STRING | No | Audit column: Upstream lineage identifier (`S3_RAW_ZONE`) |
  | `tx_date` | STRING | Partition | Partition key formatted as `YYYY-MM-DD` |

### 2.3 Gold Zone (`s3://<project>-gold-zone-<env>/`)
- **Purpose**: Curated business data marts optimized for analytical query performance and dimensional reporting.
- **File Format**: Snappy-compressed Apache Parquet.

#### Dataset 1: `gold_daily_store_sales` (Partitioned by `tx_date=YYYY-MM-DD`)
| Column Name | Data Type | Description |
|---|---|---|
| `store_id` | STRING | Store identifier |
| `total_transactions` | BIGINT | Count of distinct completed transactions |
| `unique_customers` | BIGINT | Distinct paying customer count |
| `total_revenue` | DOUBLE | Gross daily sales revenue in USD |
| `avg_order_value` | DOUBLE | Mean sales revenue per transaction |
| `total_units_sold` | BIGINT | Gross units sold |
| `ingested_at` | TIMESTAMP | Audit timestamp |
| `data_source` | STRING | Lineage source (`SILVER_TRANSACTIONS`) |
| `tx_date` | STRING (Partition) | Date of store operations |

#### Dataset 2: `gold_customer_metrics` (Customer 360 RFM Mart)
| Column Name | Data Type | Description |
|---|---|---|
| `customer_id` | STRING | Customer identifier |
| `total_orders` | BIGINT | Lifetime total order count (Frequency) |
| `lifetime_spend` | DOUBLE | Lifetime cumulative expenditure in USD (Monetary) |
| `avg_basket_value` | DOUBLE | Average expenditure per order |
| `last_order_date` | DATE | Most recent purchase date (Recency) |
| `first_order_date` | DATE | Customer acquisition transaction date |
| `ingested_at` | TIMESTAMP | Audit timestamp |
| `data_source` | STRING | Lineage source (`SILVER_TRANSACTIONS`) |

---

## 3. Data Transformation & Quality Framework

1. **Automated Schema Enforcement**: Defined with explicit PySpark `StructType` and PyArrow schemas preventing schema drift.
2. **Quality Checks**:
   - Primary key non-null validation.
   - Exact duplicate removal based on `transaction_id`.
   - Missing numerical imputation (`quantity = 1`, `total_amount = quantity * unit_price`).
   - String normalization (`TRIM` and `UPPER` on payment methods).
3. **Data Quality Audit Metric Collection**:
   - Each job calculates record counts, duplicate counts, and null counts across critical keys before persisting to S3.
4. **Idempotent Partition Overwrites**:
   - Both PySpark dynamic partition overwrites and direct PyArrow partition cleanup ensure zero duplicate records when backfilling or re-running pipelines.

---

## 4. Orchestration & Monitoring (Apache Airflow / MWAA)

The platform supports a **Dual-Mode Execution Architecture**:
- **Direct Engine (`EXECUTION_ENGINE="direct"`)**: Native Airflow execution via `pandas`, `pyarrow`, and `boto3`, executing transformations directly against S3 with sub-second execution speeds without requiring external Spark infrastructure.
- **AWS Glue Engine (`EXECUTION_ENGINE="glue"`)**: Enterprise serverless distributed computing using AWS Glue 4.0 PySpark jobs and AWS Glue Crawlers.

### Pipeline DAG Workflow (`aws_de_lakehouse_etl_pipeline`):
1. `wait_for_raw_s3_data`: `S3KeySensor` detects partition files landed for the execution date in `s3://<raw-bucket>/raw/transactions/`.
2. `run_bronze_to_silver_transformation`: Cleanses, standardizes, deduplicates, and writes conformed Snappy Parquet to Silver.
3. `run_silver_to_gold_aggregation`: Computes store performance metrics and customer RFM analytics and writes to Gold.
4. `sync_athena_partitions`: Executes `MSCK REPAIR TABLE` or triggers Glue Crawlers to register newly generated S3 partitions in the Glue Data Catalog.
5. `send_pipeline_success_notification`: Dispatches execution metadata and success alerts to the Amazon SNS topic.
6. `on_failure_callback`: Automatically captures task failure details and dispatches failure notifications via SNS.

---

## 5. Amazon Athena Analytics Models (`sql/`)

- `sql/ddl/`:
  - `01_create_silver_transactions.sql`
  - `02_create_gold_daily_store_sales.sql`
  - `03_create_gold_customer_metrics.sql`
- `sql/analytics/`:
  - `01_daily_store_sales_performance.sql`: Store rankings and 7-day moving average revenue.
  - `02_customer_rfm_segmentation.sql`: RFM loyalty scoring (`CHAMPION`, `LOYAL_CUSTOMER`, `AT_RISK_HIGH_VALUE`, `HIBERNATING_OR_LOST`).
  - `03_data_quality_reconciliation.sql`: Reconciles row counts and revenue balance between Silver and Gold tiers.

---

## 6. Infrastructure-as-Code (Terraform)

The Terraform configuration in `terraform/` provisions:
- **5 S3 Buckets**: Raw, Silver, Gold, Scripts, and Athena Results.
- **S3 Security**: Default SSE-AES256 encryption, public access block, and Glacier lifecycle policy.
- **IAM**: Least-privilege roles for Glue and Lambda.
- **Lambda Function**: Packaged deployment for real-time landing.
- **Glue Resources**: Catalog database, Silver & Gold crawlers, and 2 PySpark Glue 4.0 jobs.
- **Athena Workgroup**: Enforced S3 query output location and pre-configured Named Queries.
- **SNS Topic**: Alert topic for pipeline monitoring.
