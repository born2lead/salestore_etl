"""
Direct Lakehouse ETL Engine (Pandas / PyArrow / Boto3).
Executes Bronze -> Silver and Silver -> Gold transformations directly against AWS S3
with identical schema, partitioning, deduplication, and audit metadata as the PySpark Glue jobs.
"""

import os
import json
import logging
from datetime import datetime, timezone
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_s3_client():
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
    return boto3.client("s3", region_name=region)


def get_athena_client():
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
    return boto3.client("athena", region_name=region)


def transform_bronze_to_silver(raw_bucket: str, silver_bucket: str, date_prefix: str = None) -> int:
    """
    Reads raw JSON batches from S3 Raw Zone, cleanses, validates, deduplicates,
    and writes partitioned Snappy Parquet to S3 Silver Zone.
    """
    s3 = get_s3_client()
    prefix = f"raw/transactions/{date_prefix}" if date_prefix else "raw/transactions/"
    logger.info(f"Listing raw JSON objects in s3://{raw_bucket}/{prefix}...")
    
    response = s3.list_objects_v2(Bucket=raw_bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    
    if not contents:
        logger.warning(f"No raw files found in s3://{raw_bucket}/{prefix}")
        return 0

    all_records = []
    for item in contents:
        key = item["Key"]
        if not key.endswith(".json"):
            continue
        logger.info(f"Reading raw object: s3://{raw_bucket}/{key}")
        obj = s3.get_object(Bucket=raw_bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            all_records.extend(parsed)
        elif isinstance(parsed, dict):
            all_records.append(parsed)

    if not all_records:
        logger.warning("No records extracted from raw JSON files.")
        return 0

    df = pd.DataFrame(all_records)
    logger.info(f"Loaded {len(df)} raw records from S3.")

    # 1. Filter null transaction IDs
    df = df[df["transaction_id"].notnull()].copy()

    # 2. Field cleaning & type casts
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["store_id"] = df["store_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0).astype(float)
    df["payment_method"] = df.get("payment_method", pd.Series(["UNKNOWN"] * len(df))).fillna("UNKNOWN").astype(str).str.strip().str.upper()

    # 3. Timestamp & tx_date
    df["tx_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
    df["tx_date"] = df["tx_timestamp"].dt.strftime("%Y-%m-%d")

    # 4. Computed amounts
    df["calculated_amount"] = df["quantity"] * df["unit_price"]
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(df["calculated_amount"]).astype(float)
    else:
        df["total_amount"] = df["calculated_amount"]

    # 5. Deduplication on transaction_id
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    # 6. Audit columns
    df["ingested_at"] = datetime.now(timezone.utc)
    df["data_source"] = "S3_RAW_ZONE"

    # Select and order final silver columns
    final_cols = [
        "transaction_id", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "total_amount", "payment_method",
        "tx_timestamp", "calculated_amount", "ingested_at", "data_source", "tx_date"
    ]
    df = df[[c for c in final_cols if c in df.columns]]

    # 7. Write partitioned Snappy Parquet to Silver S3 Zone (Partition Overwrite)
    total_written = 0
    for tx_date, group in df.groupby("tx_date"):
        partition_df = group.drop(columns=["tx_date"])
        table = pa.Table.from_pandas(partition_df, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="SNAPPY")
        buffer.seek(0)

        part_prefix = f"silver/transactions/tx_date={tx_date}/"
        for old_obj in s3.list_objects_v2(Bucket=silver_bucket, Prefix=part_prefix).get("Contents", []):
            if old_obj["Key"].endswith(".parquet"):
                s3.delete_object(Bucket=silver_bucket, Key=old_obj["Key"])

        s3_key = f"{part_prefix}part-direct-{datetime.now().strftime('%Y%m%d%H%M%S')}.snappy.parquet"
        logger.info(f"Writing {len(group)} records to s3://{silver_bucket}/{s3_key}")
        s3.put_object(Bucket=silver_bucket, Key=s3_key, Body=buffer.getvalue())
        total_written += len(group)

    logger.info(f"Bronze -> Silver direct transformation complete. Total records written: {total_written}")
    return total_written


def transform_silver_to_gold(silver_bucket: str, gold_bucket: str) -> dict:
    """
    Reads cleansed transactions from S3 Silver Zone, builds:
    1. Daily Store Sales Summary (partitioned by tx_date)
    2. Customer 360 RFM metrics
    And writes Snappy Parquet to S3 Gold Zone.
    """
    s3 = get_s3_client()
    prefix = "silver/transactions/"
    logger.info(f"Reading Silver Parquet data from s3://{silver_bucket}/{prefix}...")

    response = s3.list_objects_v2(Bucket=silver_bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    silver_dfs = []
    for item in contents:
        key = item["Key"]
        if not key.endswith(".parquet"):
            continue
        
        # Extract tx_date partition from key (e.g., silver/transactions/tx_date=2026-09-25/...)
        tx_date_val = None
        for part in key.split("/"):
            if part.startswith("tx_date="):
                tx_date_val = part.split("=")[1]
                break

        obj = s3.get_object(Bucket=silver_bucket, Key=key)
        buffer = io.BytesIO(obj["Body"].read())
        sub_df = pd.read_parquet(buffer)
        if tx_date_val and "tx_date" not in sub_df.columns:
            sub_df["tx_date"] = tx_date_val
        silver_dfs.append(sub_df)

    if not silver_dfs:
        logger.warning(f"No Silver Parquet files found in s3://{silver_bucket}/{prefix}")
        return {"daily_sales_records": 0, "customer_metrics_records": 0}

    df = pd.concat(silver_dfs, ignore_index=True)
    logger.info(f"Total Silver records loaded: {len(df)}")

    now_utc = datetime.now(timezone.utc)

    # 1. Gold: Daily Store Sales Performance
    daily_sales = df.groupby(["store_id", "tx_date"]).agg(
        total_transactions=("transaction_id", "nunique"),
        unique_customers=("customer_id", "nunique"),
        total_revenue=("total_amount", lambda x: round(float(x.sum()), 2)),
        avg_order_value=("total_amount", lambda x: round(float(x.mean()), 2)),
        total_units_sold=("quantity", "sum")
    ).reset_index()

    daily_sales["ingested_at"] = now_utc
    daily_sales["data_source"] = "SILVER_TRANSACTIONS"

    # Write Daily Store Sales to S3 Gold (Partition Overwrite)
    for tx_date, group in daily_sales.groupby("tx_date"):
        part_df = group.drop(columns=["tx_date"])
        table = pa.Table.from_pandas(part_df, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="SNAPPY")
        buffer.seek(0)

        part_prefix = f"gold/daily_store_sales/tx_date={tx_date}/"
        for old_obj in s3.list_objects_v2(Bucket=gold_bucket, Prefix=part_prefix).get("Contents", []):
            if old_obj["Key"].endswith(".parquet"):
                s3.delete_object(Bucket=gold_bucket, Key=old_obj["Key"])

        s3_key = f"{part_prefix}part-direct-{now_utc.strftime('%Y%m%d%H%M%S')}.snappy.parquet"
        logger.info(f"Writing Gold Daily Store Sales: s3://{gold_bucket}/{s3_key}")
        s3.put_object(Bucket=gold_bucket, Key=s3_key, Body=buffer.getvalue())

    # 2. Gold: Customer 360 RFM Metrics
    customer_metrics = df.groupby("customer_id").agg(
        total_orders=("transaction_id", "nunique"),
        lifetime_spend=("total_amount", lambda x: round(float(x.sum()), 2)),
        avg_basket_value=("total_amount", lambda x: round(float(x.mean()), 2)),
        last_order_date=("tx_date", "max"),
        first_order_date=("tx_date", "min")
    ).reset_index()

    customer_metrics["last_order_date"] = pd.to_datetime(customer_metrics["last_order_date"]).dt.date
    customer_metrics["first_order_date"] = pd.to_datetime(customer_metrics["first_order_date"]).dt.date
    customer_metrics["ingested_at"] = now_utc
    customer_metrics["data_source"] = "SILVER_TRANSACTIONS"

    cust_schema = pa.schema([
        ("customer_id", pa.string()),
        ("total_orders", pa.int64()),
        ("lifetime_spend", pa.float64()),
        ("avg_basket_value", pa.float64()),
        ("last_order_date", pa.date32()),
        ("first_order_date", pa.date32()),
        ("ingested_at", pa.timestamp("us")),
        ("data_source", pa.string())
    ])
    table_cust = pa.Table.from_pandas(customer_metrics, schema=cust_schema, preserve_index=False)
    buf_cust = io.BytesIO()
    pq.write_table(table_cust, buf_cust, compression="SNAPPY")
    buf_cust.seek(0)

    # Clean existing parquet files to prevent schema mismatch with Athena
    existing_cust_objs = s3.list_objects_v2(Bucket=gold_bucket, Prefix="gold/customer_metrics/").get("Contents", [])
    for obj_item in existing_cust_objs:
        if obj_item["Key"].endswith(".parquet"):
            s3.delete_object(Bucket=gold_bucket, Key=obj_item["Key"])

    s3_key_cust = f"gold/customer_metrics/part-direct-{now_utc.strftime('%Y%m%d%H%M%S')}.snappy.parquet"
    logger.info(f"Writing Gold Customer Metrics: s3://{gold_bucket}/{s3_key_cust}")
    s3.put_object(Bucket=gold_bucket, Key=s3_key_cust, Body=buf_cust.getvalue())

    logger.info("Silver -> Gold direct transformation complete.")
    return {
        "daily_sales_records": len(daily_sales),
        "customer_metrics_records": len(customer_metrics)
    }


def repair_athena_tables(database_name: str, athena_bucket: str, workgroup_name: str = "primary"):
    """
    Executes MSCK REPAIR TABLE on silver_transactions and gold_daily_store_sales
    to register newly created partitions in the AWS Glue Data Catalog via Athena.
    """
    import time
    athena = get_athena_client()
    s3_output = f"s3://{athena_bucket}/athena-repairs/"
    tables = ["silver_transactions", "gold_daily_store_sales"]

    for table in tables:
        query = f"MSCK REPAIR TABLE {database_name}.{table};"
        logger.info(f"Executing Athena catalog repair: {query}")
        try:
            start_resp = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database_name},
                ResultConfiguration={"OutputLocation": s3_output},
                WorkGroup=workgroup_name
            )
            query_id = start_resp["QueryExecutionId"]

            # Wait for completion
            for _ in range(30):
                time.sleep(2)
                status_resp = athena.get_query_execution(QueryExecutionId=query_id)
                state = status_resp["QueryExecution"]["Status"]["State"]
                if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    logger.info(f"Repair query for '{table}' finished with state: {state}")
                    break
        except Exception as e:
            logger.warning(f"Athena repair for table {table} encountered: {e}")
