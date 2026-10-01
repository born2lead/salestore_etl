"""
Automated Athena Data Sync & Analytics Execution Script.

Performs:
1. Uploads local Silver & Gold Parquet tables to S3 Lakehouse Buckets.
2. Registers External DDL Tables in Glue/Athena Database ('de_proj_aws_db_dev').
3. Runs MSCK REPAIR TABLE to populate partition metadata.
4. Executes and displays results for all 3 Lakehouse Analytics queries:
   - Daily Store Sales Performance & Rolling 7-Day Average
   - Customer 360 RFM Segmentation
   - Medallion Data Quality Reconciliation Audit
"""

import time
import boto3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_BUCKET = "de-proj-aws-silver-zone-dev"
GOLD_BUCKET = "de-proj-aws-gold-zone-dev"
ATHENA_RESULTS_BUCKET = "de-proj-aws-athena-results-dev"
DB_NAME = "de_proj_aws_db_dev"
WORKGROUP = "de-proj-aws-workgroup-dev"

s3_client = boto3.client("s3")
athena_client = boto3.client("athena", region_name="ap-south-1")

def sync_local_parquet_to_s3():
    """Upload local Silver and Gold Parquet files to S3 buckets."""
    print("[1/4] Syncing local Parquet data to S3 Lakehouse buckets...")
    
    # 1. Silver transactions
    silver_local = PROJECT_ROOT / "data" / "silver"
    if silver_local.exists():
        for file in silver_local.glob("**/*"):
            if file.is_file() and not file.name.startswith("."):
                rel_path = file.relative_to(silver_local)
                s3_key = f"silver/transactions/{rel_path.as_posix()}"
                print(f"  + Uploading Silver: {rel_path} -> s3://{SILVER_BUCKET}/{s3_key}")
                s3_client.upload_file(str(file), SILVER_BUCKET, s3_key)
                
    # 2. Gold store sales & customer metrics
    gold_local = PROJECT_ROOT / "data" / "gold"
    if gold_local.exists():
        for file in gold_local.glob("**/*"):
            if file.is_file() and not file.name.startswith("."):
                rel_path = file.relative_to(gold_local)
                s3_key = f"gold/{rel_path.as_posix()}"
                print(f"  + Uploading Gold: {rel_path} -> s3://{GOLD_BUCKET}/{s3_key}")
                s3_client.upload_file(str(file), GOLD_BUCKET, s3_key)
                
    print("S3 Data Sync complete.\n")

def run_athena_query(query_sql: str, description: str) -> dict:
    """Execute query in Athena workgroup and wait for execution completion."""
    print(f"Executing Athena Query: {description}...")
    response = athena_client.start_query_execution(
        QueryString=query_sql,
        QueryExecutionContext={"Database": DB_NAME},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/results/"}
    )
    
    query_id = response["QueryExecutionId"]
    
    # Poll query status
    while True:
        status_resp = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)
        
    if state != "SUCCEEDED":
        reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise RuntimeError(f"Athena query failed ({state}): {reason}")
        
    print(f"  Query {query_id} SUCCEEDED!")
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    return results

def clean_sql_statement(sql: str) -> str:
    """Remove SQL comments and extra whitespace to ensure a single valid query statement."""
    lines = []
    for line in sql.splitlines():
        line_strip = line.strip()
        if line_strip.startswith("--") or not line_strip:
            continue
        # Remove inline comments if present
        if "--" in line:
            line = line.split("--")[0]
        lines.append(line)
    return "\n".join(lines).strip().rstrip(";")

def register_ddl_tables():
    """Create external DDL tables in Athena/Glue Database."""
    print("[2/4] Registering Athena External Tables...")
    
    # Drop existing tables to refresh schema definitions
    run_athena_query(f"DROP TABLE IF EXISTS {DB_NAME}.silver_transactions;", "Drop silver_transactions table")
    run_athena_query(f"DROP TABLE IF EXISTS {DB_NAME}.gold_daily_store_sales;", "Drop gold_daily_store_sales table")
    run_athena_query(f"DROP TABLE IF EXISTS {DB_NAME}.gold_customer_metrics;", "Drop gold_customer_metrics table")
    
    ddl_dir = PROJECT_ROOT / "sql" / "ddl"
    ddl_files = sorted(list(ddl_dir.glob("*.sql")))
    
    for ddl_file in ddl_files:
        sql = ddl_file.read_text(encoding="utf-8")
        # Template replacements
        sql = sql.replace("{{ database_name }}", DB_NAME)
        sql = sql.replace("{{ silver_bucket }}", SILVER_BUCKET)
        sql = sql.replace("{{ gold_bucket }}", GOLD_BUCKET)
        
        cleaned_sql = clean_sql_statement(sql)
        run_athena_query(cleaned_sql, f"DDL Table Creation ({ddl_file.name})")

def repair_partitions():
    """Run MSCK REPAIR TABLE to register partition keys in Glue Catalog."""
    print("[3/4] Repairing Table Partitions (MSCK REPAIR TABLE)...")
    run_athena_query(f"MSCK REPAIR TABLE {DB_NAME}.silver_transactions;", "Repair Silver Partitions")
    run_athena_query(f"MSCK REPAIR TABLE {DB_NAME}.gold_daily_store_sales;", "Repair Gold Store Sales Partitions")
    print("Partitions registered successfully.\n")

def display_query_results(results: dict, title: str):
    """Format and print query results nicely."""
    print(f"\n============================================================================")
    print(f" RESULTS: {title}")
    print(f"============================================================================")
    
    rows = results["ResultSet"]["Rows"]
    if not rows:
        print("No rows returned.")
        return
        
    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    header_line = " | ".join(f"{h:<22}" for h in headers)
    print(header_line)
    print("-" * len(header_line))
    
    for row in rows[1:]:
        vals = [col.get("VarCharValue", "NULL") for col in row["Data"]]
        print(" | ".join(f"{v:<22}" for v in vals))
    print("============================================================================\n")

def execute_analytics_queries():
    """Run all 3 analytical queries and print output."""
    print("[4/4] Executing Analytical Queries...")
    
    analytics_dir = PROJECT_ROOT / "sql" / "analytics"
    
    # 1. Daily Store Sales Performance
    file_01 = analytics_dir / "01_daily_store_sales_performance.sql"
    if file_01.exists():
        sql = clean_sql_statement(file_01.read_text(encoding="utf-8"))
        res = run_athena_query(sql, "01 Daily Store Sales Performance")
        display_query_results(res, "Daily Store Sales Performance")
        
    # 2. Customer 360 RFM Segmentation
    file_02 = analytics_dir / "02_customer_rfm_segmentation.sql"
    if file_02.exists():
        sql = clean_sql_statement(file_02.read_text(encoding="utf-8"))
        res = run_athena_query(sql, "02 Customer 360 RFM Segmentation")
        display_query_results(res, "Customer 360 RFM Segmentation")
        
    # 3. Data Quality Reconciliation Audit
    file_03 = analytics_dir / "03_data_quality_reconciliation.sql"
    if file_03.exists():
        sql = clean_sql_statement(file_03.read_text(encoding="utf-8"))
        res = run_athena_query(sql, "03 Data Quality Reconciliation Audit")
        display_query_results(res, "Data Quality Reconciliation Audit")

if __name__ == "__main__":
    sync_local_parquet_to_s3()
    register_ddl_tables()
    repair_partitions()
    execute_analytics_queries()
