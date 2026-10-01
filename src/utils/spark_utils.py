"""
Spark Utilities for AWS Data Engineering Pipeline.
Handles Spark session initialization, schema definitions, data quality checks, and S3 partitioning logic.
"""

import sys
import os
import logging
from typing import Dict, Any, List

# Ensure PySpark workers use the active virtualenv Python executable and local Hadoop binaries on Windows
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
hadoop_dir = os.path.join(base_dir, "hadoop")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ.get("JAVA_HOME", "")):
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jre1.8.0_491"

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, count, when

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "AWS-DE-Pipeline-Spark") -> SparkSession:
    """
    Initializes or retrieves an active PySpark Session configured for AWS Glue / S3 environment.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    spark = builder.getOrCreate()
    logger.info(f"Spark Session successfully initialized for application: {app_name}")
    return spark


def add_audit_metadata(df: DataFrame, source_system: str = "AWS_DE_Pipeline") -> DataFrame:
    """
    Appends standard audit metadata columns (ingestion timestamp, record source).
    """
    return df \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("data_source", lit(source_system))


def validate_data_quality(df: DataFrame, mandatory_columns: List[str]) -> Dict[str, Any]:
    """
    Performs basic data quality checks on the DataFrame:
    - Row count validation
    - Null check on mandatory columns
    - Duplicate detection
    """
    total_records = df.count()
    quality_summary = {
        "total_records": total_records,
        "null_counts": {},
        "duplicate_count": 0,
        "passed": True
    }
    
    if total_records == 0:
        logger.warning("Data Quality Warning: Received empty DataFrame!")
        quality_summary["passed"] = False
        return quality_summary

    # Check nulls on mandatory columns
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in mandatory_columns if c in df.columns]
    if null_exprs:
        null_counts = df.select(null_exprs).collect()[0].asDict()
        quality_summary["null_counts"] = null_counts
        for col_name, null_cnt in null_counts.items():
            if null_cnt > 0:
                logger.warning(f"Data Quality Alert: Column '{col_name}' has {null_cnt} null values.")

    # Duplicate check based on primary key or mandatory fields
    distinct_records = df.select(mandatory_columns).distinct().count()
    duplicate_count = total_records - distinct_records
    quality_summary["duplicate_count"] = duplicate_count
    
    if duplicate_count > 0:
        logger.info(f"Data Quality Notice: Detected {duplicate_count} duplicate key records.")

    logger.info(f"Data Quality Summary: {quality_summary}")
    return quality_summary


def write_partitioned_parquet(
    df: DataFrame,
    target_s3_path: str,
    partition_cols: List[str] = None,
    mode: str = "overwrite"
) -> None:
    """
    Writes a DataFrame to S3 in Parquet format with snappy compression and dynamic partitioning.
    """
    writer = df.write.mode(mode).format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    logger.info(f"Writing dataset to S3 path: {target_s3_path} with mode={mode} and partitions={partition_cols}")
    writer.save(target_s3_path)
    logger.info(f"Successfully wrote data to {target_s3_path}")


def parse_job_arguments(expected_args: List[str], defaults: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Parses job arguments for both AWS Glue environment and local/CLI execution.
    In AWS Glue, utilizes awsglue.utils.getResolvedOptions.
    In local or test runs, parses command line flags (e.g. --raw_path) or falls back to defaults.
    """
    defaults = defaults or {}
    try:
        from awsglue.utils import getResolvedOptions
        return getResolvedOptions(sys.argv, expected_args)
    except (ImportError, Exception):
        import argparse
        parser = argparse.ArgumentParser()
        for arg in expected_args:
            parser.add_argument(f"--{arg}", dest=arg, default=defaults.get(arg), required=False)
        parsed, unknown = parser.parse_known_args()
        result = vars(parsed)
        # Check positional arguments if not specified with -- flags
        positional = [u for u in unknown if not u.startswith("-")]
        for idx, arg in enumerate(expected_args):
            if result.get(arg) is None:
                if idx < len(positional):
                    result[arg] = positional[idx]
                else:
                    result[arg] = defaults.get(arg)
        return result

