"""
Unit Tests for PySpark ETL Transformations (Bronze->Silver & Silver->Gold).
Uses local PySpark session for automated unit testing.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, trim, upper, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.utils.spark_utils import add_audit_metadata, validate_data_quality, parse_job_arguments
from src.etl.glue_silver_to_gold import build_daily_store_sales_gold, build_customer_metrics_gold


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .master("local[2]") \
        .appName("PySpark-Unit-Tests") \
        .getOrCreate()
    yield session
    session.stop()


def test_add_audit_metadata(spark):
    data = [("TX1001", "CUST01", 100.50)]
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    result_df = add_audit_metadata(df, source_system="TEST_SOURCE")
    
    assert "ingested_at" in result_df.columns
    assert "data_source" in result_df.columns
    assert result_df.collect()[0]["data_source"] == "TEST_SOURCE"


def test_validate_data_quality(spark):
    data = [
        ("TX1001", "CUST01", 100.0),
        ("TX1002", "CUST02", 200.0),
        (None, "CUST03", 50.0)
    ]
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    dq = validate_data_quality(df, mandatory_columns=["transaction_id"])
    
    assert dq["total_records"] == 3
    assert dq["null_counts"]["transaction_id"] == 1


def test_build_daily_store_sales_gold(spark):
    data = [
        ("S001", "2026-09-25", "TX01", "C01", 2, 50.0),
        ("S001", "2026-09-25", "TX02", "C02", 1, 30.0),
    ]
    schema = StructType([
        StructField("store_id", StringType(), True),
        StructField("tx_date", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    silver_df = spark.createDataFrame(data, schema)
    gold_df = build_daily_store_sales_gold(silver_df)
    
    row = gold_df.collect()[0]
    assert row["store_id"] == "S001"
    assert row["total_transactions"] == 2
    assert row["total_revenue"] == 80.0
    assert row["total_units_sold"] == 3


def test_build_customer_metrics_gold(spark):
    data = [
        ("TX01", "CUST01", "2026-09-20", 50.0),
        ("TX02", "CUST01", "2026-09-25", 150.0),
        ("TX03", "CUST02", "2026-09-22", 80.0),
    ]
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("tx_date", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    silver_df = spark.createDataFrame(data, schema)
    customer_metrics_df = build_customer_metrics_gold(silver_df)
    
    results = {row["customer_id"]: row for row in customer_metrics_df.collect()}
    
    cust1 = results["CUST01"]
    assert cust1["total_orders"] == 2
    assert cust1["lifetime_spend"] == 200.0
    assert cust1["avg_basket_value"] == 100.0
    assert cust1["first_order_date"] == "2026-09-20"
    assert cust1["last_order_date"] == "2026-09-25"
    
    cust2 = results["CUST02"]
    assert cust2["total_orders"] == 1
    assert cust2["lifetime_spend"] == 80.0


def test_bronze_to_silver_cleaning_and_deduplication(spark):
    # Tests schema conformance, deduplication on transaction_id, and calculated amount
    raw_data = [
        ("TX100 ", " CUST01 ", "STORE1", 2, 25.0, None, "2026-09-25T10:30:00Z", " card "),
        ("TX100 ", " CUST01 ", "STORE1", 2, 25.0, 50.0, "2026-09-25T10:30:00Z", " card "), # Duplicate TX100
        (None, "CUST02", "STORE2", 1, 10.0, 10.0, "2026-09-25T11:00:00Z", "CASH"),         # Null TX ID
        ("TX101", "CUST03", "STORE1", None, 15.0, None, "2026-09-26T09:15:00Z", None),       # Default quantity
    ]
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    raw_df = spark.createDataFrame(raw_data, schema)
    
    # Apply cleansing transformation logic
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
        
    deduped_df = cleaned_df.dropDuplicates(["transaction_id"])
    
    rows = deduped_df.collect()
    assert len(rows) == 2  # Deduplicated from 4 rows (1 null dropped, 1 duplicate dropped)
    
    record_map = {r["transaction_id"]: r for r in rows}
    assert "TX100" in record_map
    assert record_map["TX100"]["customer_id"] == "CUST01"
    assert record_map["TX100"]["payment_method"] == "CARD"
    assert record_map["TX100"]["total_amount"] == 50.0
    
    assert "TX101" in record_map
    assert record_map["TX101"]["quantity"] == 1  # defaulted to 1
    assert record_map["TX101"]["total_amount"] == 15.0
    assert record_map["TX101"]["payment_method"] == "UNKNOWN"


def test_parse_job_arguments():
    defaults = {
        "raw_path": "s3://default-raw/",
        "silver_path": "s3://default-silver/"
    }
    args = parse_job_arguments(["raw_path", "silver_path"], defaults=defaults)
    assert "raw_path" in args
    assert "silver_path" in args
    assert args["raw_path"] == "s3://default-raw/"
