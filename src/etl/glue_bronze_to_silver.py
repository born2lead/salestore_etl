"""
AWS Glue PySpark Job: Bronze to Silver Transformation.
Reads raw JSON/CSV data from S3 Raw Zone, enforces schema, handles data cleansing,
deduplication, quality checks, and writes partitioned Parquet files into S3 Silver Zone.
"""

import sys
import os
from pyspark.sql.functions import (
    col, to_timestamp, to_date, trim, upper, coalesce, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# Enable importing local utils if bundled in Glue extra-py-files zip
try:
    from utils.spark_utils import (
        get_spark_session, add_audit_metadata, validate_data_quality,
        write_partitioned_parquet, parse_job_arguments
    )
except ImportError:
    from src.utils.spark_utils import (
        get_spark_session, add_audit_metadata, validate_data_quality,
        write_partitioned_parquet, parse_job_arguments
    )

def process_bronze_to_silver(raw_s3_path: str, silver_s3_path: str):
    spark = get_spark_session(app_name="Glue-BronzeToSilver-Job")
    
    # 1. Define explicit target schema for raw transactions
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    print(f"Reading raw data from S3 path: {raw_s3_path}")
    raw_df = spark.read.option("multiline", "true").schema(schema).json(raw_s3_path)
    
    if raw_df.rdd.isEmpty():
        print("No records found in raw path. Exiting Bronze to Silver job.")
        return
        
    # 2. Data Cleaning & Transformations
    cleaned_df = raw_df \
        .filter(col("transaction_id").isNotNull()) \
        .withColumn("transaction_id", trim(col("transaction_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("payment_method", upper(trim(coalesce(col("payment_method"), lit("UNKNOWN"))))) \
        .withColumn("tx_timestamp", to_timestamp(col("transaction_timestamp"))) \
        .withColumn("tx_date", to_date(col("tx_timestamp"))) \
        .withColumn("quantity", coalesce(col("quantity"), lit(1))) \
        .withColumn("calculated_amount", col("quantity") * col("unit_price")) \
        .withColumn("total_amount", coalesce(col("total_amount"), col("calculated_amount"))) \
        .drop("transaction_timestamp")
        
    # 3. Deduplication on Transaction ID
    deduped_df = cleaned_df.dropDuplicates(["transaction_id"])
    
    # 4. Add Audit Columns
    enriched_df = add_audit_metadata(deduped_df, source_system="S3_RAW_ZONE")
    
    # 5. Data Quality Checks
    dq_result = validate_data_quality(enriched_df, mandatory_columns=["transaction_id", "customer_id", "tx_date"])
    print(f"Bronze -> Silver DQ Assessment: {dq_result}")
    
    # 6. Write to S3 Silver Zone (Partitioned by tx_date)
    write_partitioned_parquet(
        df=enriched_df,
        target_s3_path=silver_s3_path,
        partition_cols=["tx_date"],
        mode="overwrite"
    )
    print("Bronze to Silver processing completed successfully.")


if __name__ == "__main__":
    defaults = {
        "raw_path": "s3://aws-de-lakehouse-raw-zone/raw/transactions/",
        "silver_path": "s3://aws-de-lakehouse-silver-zone/silver/transactions/"
    }
    args = parse_job_arguments(["raw_path", "silver_path"], defaults=defaults)
    process_bronze_to_silver(args["raw_path"], args["silver_path"])
