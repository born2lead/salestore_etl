"""
Unit Tests for Airflow DAG Structural Integrity.
Validates the DAG file syntax, task identifiers, configuration constants, and dependencies.
"""

import ast
import os


def test_dag_file_syntax_and_structure():
    dag_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dags",
        "aws_de_pipeline_dag.py"
    )
    assert os.path.exists(dag_path), "DAG file must exist"

    with open(dag_path, "r", encoding="utf-8") as f:
        dag_content = f.read()

    # 1. Parse AST to guarantee Python syntax correctness
    parsed_ast = ast.parse(dag_content, filename=dag_path)
    assert isinstance(parsed_ast, ast.Module)

    # 2. Check essential task definitions in DAG
    expected_tasks = [
        "wait_for_raw_s3_data",
        "run_glue_bronze_to_silver",
        "run_glue_silver_to_gold",
        "trigger_athena_silver_crawler",
        "trigger_athena_gold_crawler",
        "send_pipeline_success_notification",
    ]
    for task_id in expected_tasks:
        assert task_id in dag_content, f"Task '{task_id}' should be defined in DAG"

    # 3. Check Airflow operators used
    assert "S3KeySensor" in dag_content
    assert "GlueJobOperator" in dag_content
    assert "GlueCrawlerOperator" in dag_content
    assert "PythonOperator" in dag_content

    # 4. Check schedule interval and alert configs
    assert "schedule_interval='0 2 * * *'" in dag_content
    assert "on_failure_callback" in dag_content
