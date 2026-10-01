"""
Local Execution Script for AWS Data Engineering ETL Pipeline.
Runs the Bronze->Silver and Silver->Gold PySpark transformations locally on sample data.
"""

import os
import shutil
from src.etl.glue_bronze_to_silver import process_bronze_to_silver
from src.etl.glue_silver_to_gold import process_silver_to_gold
from src.utils.spark_utils import get_spark_session


def run_local_pipeline():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, "data", "sample_raw_transactions.json")
    silver_path = os.path.join(base_dir, "data", "silver")
    gold_path = os.path.join(base_dir, "data", "gold")

    print(f"=== Step 1: Processing Bronze -> Silver ===")
    process_bronze_to_silver(raw_path, silver_path)
    
    print(f"\n=== Step 2: Processing Silver -> Gold ===")
    process_silver_to_gold(silver_path, gold_path)
    
    print(f"\n=== Step 3: Inspecting Gold Datasets ===")
    spark = get_spark_session(app_name="Local-Gold-Inspector")
    
    daily_sales_path = os.path.join(gold_path, "daily_store_sales")
    if os.path.exists(daily_sales_path):
        print("\n--- Gold Zone: Daily Store Sales Aggregation ---")
        df_sales = spark.read.parquet(daily_sales_path)
        df_sales.show(truncate=False)
        
    customer_metrics_path = os.path.join(gold_path, "customer_metrics")
    if os.path.exists(customer_metrics_path):
        print("\n--- Gold Zone: Customer Metrics Aggregation ---")
        df_cust = spark.read.parquet(customer_metrics_path)
        df_cust.show(truncate=False)


if __name__ == "__main__":
    run_local_pipeline()
