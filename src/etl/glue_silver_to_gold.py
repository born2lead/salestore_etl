"""
AWS Glue PySpark Job: Silver to Gold Transformation.
Reads cleansed transactions from S3 Silver Zone, builds aggregated business metrics
(Daily Store Sales Summary & Customer Purchasing RFM Metrics), and saves into S3 Gold Zone.
"""

import sys
from pyspark.sql.functions import (
    col, sum, count, countDistinct, avg, round, max, min, current_timestamp, date_format
)

try:
    from utils.spark_utils import (
        get_spark_session, add_audit_metadata, write_partitioned_parquet, parse_job_arguments
    )
except ImportError:
    from src.utils.spark_utils import (
        get_spark_session, add_audit_metadata, write_partitioned_parquet, parse_job_arguments
    )


def build_daily_store_sales_gold(silver_df) -> tuple:
    """
    Aggregates metrics by Store ID and Transaction Date.
    """
    daily_sales = silver_df.groupBy("store_id", "tx_date") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            countDistinct("customer_id").alias("unique_customers"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            sum("quantity").alias("total_units_sold")
        )
    return add_audit_metadata(daily_sales, source_system="SILVER_TRANSACTIONS")


def build_customer_metrics_gold(silver_df) -> tuple:
    """
    Computes Customer Aggregations (Recency, Frequency, Monetary summary).
    """
    customer_gold = silver_df.groupBy("customer_id") \
        .agg(
            count("transaction_id").alias("total_orders"),
            round(sum("total_amount"), 2).alias("lifetime_spend"),
            round(avg("total_amount"), 2).alias("avg_basket_value"),
            max("tx_date").alias("last_order_date"),
            min("tx_date").alias("first_order_date")
        )
    return add_audit_metadata(customer_gold, source_system="SILVER_TRANSACTIONS")


def process_silver_to_gold(silver_s3_path: str, gold_s3_base_path: str):
    spark = get_spark_session(app_name="Glue-SilverToGold-Job")
    
    print(f"Reading cleansed dataset from Silver S3 path: {silver_s3_path}")
    silver_df = spark.read.parquet(silver_s3_path)
    
    if silver_df.rdd.isEmpty():
        print("Silver zone data is empty. Exiting Silver to Gold job.")
        return
        
    # 1. Generate Store Daily Sales Aggregations
    daily_store_sales_df = build_daily_store_sales_gold(silver_df)
    gold_store_path = f"{gold_s3_base_path.rstrip('/')}/daily_store_sales/"
    
    write_partitioned_parquet(
        df=daily_store_sales_df,
        target_s3_path=gold_store_path,
        partition_cols=["tx_date"],
        mode="overwrite"
    )
    
    # 2. Generate Customer 360 RFM Summary
    customer_metrics_df = build_customer_metrics_gold(silver_df)
    gold_customer_path = f"{gold_s3_base_path.rstrip('/')}/customer_metrics/"
    
    write_partitioned_parquet(
        df=customer_metrics_df,
        target_s3_path=gold_customer_path,
        partition_cols=None,
        mode="overwrite"
    )
    
    print("Silver to Gold aggregation pipeline completed successfully.")


if __name__ == "__main__":
    defaults = {
        "silver_path": "s3://aws-de-lakehouse-silver-zone/silver/transactions/",
        "gold_path": "s3://aws-de-lakehouse-gold-zone/gold/"
    }
    args = parse_job_arguments(["silver_path", "gold_path"], defaults=defaults)
    process_silver_to_gold(args["silver_path"], args["gold_path"])
