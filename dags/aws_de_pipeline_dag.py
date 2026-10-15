"""
AWS Data Engineering ETL Pipeline Orchestration DAG (Apache Airflow / MWAA).
Orchestrates Raw Ingestion Check -> Glue Bronze-to-Silver -> Glue Silver-to-Gold -> Athena Catalog Crawlers -> SNS Alerting.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Default arguments for Airflow Tasks
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Pipeline Configs
RAW_S3_BUCKET = os.environ.get("RAW_S3_BUCKET", "de-proj-aws-raw-zone-dev")
SILVER_S3_BUCKET = os.environ.get("SILVER_S3_BUCKET", "de-proj-aws-silver-zone-dev")
GOLD_S3_BUCKET = os.environ.get("GOLD_S3_BUCKET", "de-proj-aws-gold-zone-dev")
RAW_S3_KEY_PATTERN = "raw/transactions/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/*.json"
GLUE_BRONZE_SILVER_JOB_NAME = os.environ.get("GLUE_BRONZE_SILVER_JOB_NAME", "de-proj-aws-bronze-to-silver")
GLUE_SILVER_GOLD_JOB_NAME = os.environ.get("GLUE_SILVER_GOLD_JOB_NAME", "de-proj-aws-silver-to-gold")
GLUE_SILVER_CRAWLER_NAME = os.environ.get("GLUE_SILVER_CRAWLER_NAME", "de-proj-aws-silver-crawler-dev")
GLUE_GOLD_CRAWLER_NAME = os.environ.get("GLUE_GOLD_CRAWLER_NAME", "de-proj-aws-gold-crawler-dev")
GLUE_IAM_ROLE_NAME = os.environ.get("GLUE_IAM_ROLE_NAME", "de-proj-aws-glue-role-dev")
SCRIPTS_S3_BUCKET = os.environ.get("SCRIPTS_S3_BUCKET", "de-proj-aws-scripts-dev")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "arn:aws:sns:ap-south-1:939139585522:de-proj-aws-pipeline-alerts-dev")
EXECUTION_ENGINE = os.environ.get("EXECUTION_ENGINE", "direct").lower()
ATHENA_RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", "de-proj-aws-athena-results-dev")
GLUE_DATABASE_NAME = os.environ.get("GLUE_DATABASE_NAME", "de_proj_aws_db_dev")
ATHENA_WORKGROUP_NAME = os.environ.get("ATHENA_WORKGROUP_NAME", "primary")


def execute_direct_bronze_to_silver(**context):
    """Executes Bronze to Silver cleaning and deduplication directly against S3."""
    from src.etl.direct_lakehouse_etl import transform_bronze_to_silver
    count = transform_bronze_to_silver(RAW_S3_BUCKET, SILVER_S3_BUCKET)
    print(f"Direct Bronze to Silver completed. Processed {count} records into s3://{SILVER_S3_BUCKET}/silver/transactions/")
    return count


def execute_direct_silver_to_gold(**context):
    """Executes Silver to Gold Daily Store Sales and Customer 360 aggregations directly against S3."""
    from src.etl.direct_lakehouse_etl import transform_silver_to_gold
    result = transform_silver_to_gold(SILVER_S3_BUCKET, GOLD_S3_BUCKET)
    print(f"Direct Silver to Gold completed: {result}")
    return result


def execute_direct_athena_sync(**context):
    """Syncs Athena external table partitions for Silver and Gold layers."""
    from src.etl.direct_lakehouse_etl import repair_athena_tables
    repair_athena_tables(GLUE_DATABASE_NAME, ATHENA_RESULTS_BUCKET, ATHENA_WORKGROUP_NAME)
    print(f"Athena tables in {GLUE_DATABASE_NAME} repaired and synced with S3.")


def notify_pipeline_completion(**context):
    """
    Sends successful completion alert to AWS SNS topic.
    """
    execution_date = context.get('ds')
    dag_run_id = context.get('run_id')
    message = {
        "status": "SUCCESS",
        "dag_id": context.get('dag').dag_id if context.get('dag') else "aws_de_lakehouse_etl_pipeline",
        "execution_date": execution_date,
        "run_id": dag_run_id,
        "details": "Raw -> Bronze -> Silver -> Gold ETL pipeline and catalog crawl completed."
    }
    try:
        from src.utils.aws_helpers import send_sns_notification
        send_sns_notification(
            topic_arn=SNS_TOPIC_ARN,
            subject=f"[SUCCESS] Lakehouse ETL Pipeline - {execution_date}",
            message_dict=message
        )
    except Exception as e:
        print(f"SNS notification log (mock/offline): {message} (Error: {str(e)})")


def notify_pipeline_failure(context):
    """
    Failure callback triggered when any task within DAG fails.
    """
    task_instance = context.get('task_instance')
    execution_date = context.get('ds')
    error_msg = {
        "status": "FAILED",
        "dag_id": context.get('dag').dag_id if context.get('dag') else "aws_de_lakehouse_etl_pipeline",
        "task_id": task_instance.task_id if task_instance else "unknown",
        "execution_date": execution_date,
        "log_url": task_instance.log_url if task_instance else "N/A"
    }
    try:
        from src.utils.aws_helpers import send_sns_notification
        send_sns_notification(
            topic_arn=SNS_TOPIC_ARN,
            subject=f"[ALERT] Lakehouse ETL Pipeline Task Failed - {task_instance.task_id if task_instance else 'task'}",
            message_dict=error_msg
        )
    except Exception as e:
        print(f"SNS failure alert log: {error_msg} (Error: {str(e)})")


default_args['on_failure_callback'] = notify_pipeline_failure


with DAG(
    dag_id='aws_de_lakehouse_etl_pipeline',
    default_args=default_args,
    description='Automated AWS Lakehouse ETL Pipeline: Raw -> Bronze -> Silver -> Gold',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['aws', 'glue', 'pyspark', 'lakehouse', 'gold', 'athena'],
) as dag:

    # 1. Wait for landing raw files in S3
    wait_for_raw_data = S3KeySensor(
        task_id='wait_for_raw_s3_data',
        bucket_name=RAW_S3_BUCKET,
        bucket_key=RAW_S3_KEY_PATTERN,
        wildcard_match=True,
        timeout=3600,
        poke_interval=120,
        mode='poke'
    )

    # 2 & 3. ETL Transformations & Catalog Sync based on EXECUTION_ENGINE
    if EXECUTION_ENGINE == "glue":
        # AWS Glue Managed Cloud Compute
        run_bronze_to_silver = GlueJobOperator(
            task_id='run_glue_bronze_to_silver',
            job_name=GLUE_BRONZE_SILVER_JOB_NAME,
            script_location=f"s3://{SCRIPTS_S3_BUCKET}/src/etl/glue_bronze_to_silver.py",
            s3_bucket=SCRIPTS_S3_BUCKET,
            iam_role_name=GLUE_IAM_ROLE_NAME,
            create_job_kwargs={
                "GlueVersion": "4.0",
                "NumberOfWorkers": 2,
                "WorkerType": "G.1X",
                "DefaultArguments": {
                    "--extra-py-files": f"s3://{SCRIPTS_S3_BUCKET}/dist/pipeline_utils.zip"
                }
            },
            script_args={
                '--raw_path': f"s3://{RAW_S3_BUCKET}/raw/transactions/",
                '--silver_path': f"s3://{SILVER_S3_BUCKET}/silver/transactions/"
            },
            wait_for_completion=True,
            verbose=True
        )

        run_silver_to_gold = GlueJobOperator(
            task_id='run_glue_silver_to_gold',
            job_name=GLUE_SILVER_GOLD_JOB_NAME,
            script_location=f"s3://{SCRIPTS_S3_BUCKET}/src/etl/glue_silver_to_gold.py",
            s3_bucket=SCRIPTS_S3_BUCKET,
            iam_role_name=GLUE_IAM_ROLE_NAME,
            create_job_kwargs={
                "GlueVersion": "4.0",
                "NumberOfWorkers": 2,
                "WorkerType": "G.1X",
                "DefaultArguments": {
                    "--extra-py-files": f"s3://{SCRIPTS_S3_BUCKET}/dist/pipeline_utils.zip"
                }
            },
            script_args={
                '--silver_path': f"s3://{SILVER_S3_BUCKET}/silver/transactions/",
                '--gold_path': f"s3://{GOLD_S3_BUCKET}/gold/"
            },
            wait_for_completion=True,
            verbose=True
        )

        sync_catalog = [
            GlueCrawlerOperator(
                task_id='trigger_athena_silver_crawler',
                config={'Name': GLUE_SILVER_CRAWLER_NAME},
                poll_interval=30
            ),
            GlueCrawlerOperator(
                task_id='trigger_athena_gold_crawler',
                config={'Name': GLUE_GOLD_CRAWLER_NAME},
                poll_interval=30
            )
        ]
    else:
        # Direct Lakehouse Engine (Airflow-native Pandas/PyArrow/Athena execution)
        run_bronze_to_silver = PythonOperator(
            task_id='run_bronze_to_silver_transformation',
            python_callable=execute_direct_bronze_to_silver,
            provide_context=True
        )

        run_silver_to_gold = PythonOperator(
            task_id='run_silver_to_gold_aggregation',
            python_callable=execute_direct_silver_to_gold,
            provide_context=True
        )

        sync_catalog = PythonOperator(
            task_id='sync_athena_partitions',
            python_callable=execute_direct_athena_sync,
            provide_context=True
        )

    # 4. Send Success Alert
    send_success_alert = PythonOperator(
        task_id='send_pipeline_success_notification',
        python_callable=notify_pipeline_completion,
        provide_context=True
    )

    # Pipeline Dependency Graph
    wait_for_raw_data >> run_bronze_to_silver >> run_silver_to_gold >> sync_catalog >> send_success_alert
