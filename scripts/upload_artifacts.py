"""
Automated Build, Package, and S3 Deployment Script for AWS DE Pipeline.

Performs:
1. Packaging: Compiles src/utils into dist/pipeline_utils.zip for AWS Glue --extra-py-files.
2. S3 Deployment: Uploads all PySpark ETL scripts and utility zip bundles to the S3 scripts bucket.
"""

import sys
import os
import zipfile
import boto3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = PROJECT_ROOT / "dist"
SRC_UTILS_DIR = PROJECT_ROOT / "src" / "utils"
ETL_DIR = PROJECT_ROOT / "src" / "etl"
DEFAULT_BUCKET = "de-proj-aws-scripts-dev"

def package_glue_utils() -> Path:
    """Package src/utils into dist/pipeline_utils.zip with 'utils' top-level directory structure."""
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DIST_DIR / "pipeline_utils.zip"
    
    print(f"[1/2] Packaging Glue utility module into {zip_path}...")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr("utils/__init__.py", "# Package marker for Glue utils\n")
        
        for py_file in SRC_UTILS_DIR.glob("*.py"):
            if py_file.name != "__init__.py":
                archive_name = f"utils/{py_file.name}"
                zipf.write(py_file, arcname=archive_name)
                print(f"  + Added {py_file.relative_to(PROJECT_ROOT)} -> {archive_name}")
                
    print(f"Successfully created {zip_path} ({zip_path.stat().st_size} bytes)\n")
    return zip_path

def upload_artifacts_to_s3(bucket_name: str = DEFAULT_BUCKET):
    """Upload packaged utility zip and ETL PySpark scripts to target S3 bucket."""
    zip_path = package_glue_utils()
    
    print(f"[2/2] Uploading pipeline artifacts to S3 bucket '{bucket_name}'...")
    s3_client = boto3.client("s3")
    
    # 1. Upload utils zip archive
    s3_zip_key = "dist/pipeline_utils.zip"
    print(f"  -> Uploading {zip_path.name} to s3://{bucket_name}/{s3_zip_key}")
    s3_client.upload_file(str(zip_path), bucket_name, s3_zip_key)
    
    # 2. Upload PySpark ETL scripts
    for etl_script in ETL_DIR.glob("*.py"):
        s3_key = f"src/etl/{etl_script.name}"
        print(f"  -> Uploading {etl_script.name} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(str(etl_script), bucket_name, s3_key)
        
    print(f"\nAll pipeline artifacts successfully deployed to s3://{bucket_name}!")

if __name__ == "__main__":
    target_bucket = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BUCKET
    upload_artifacts_to_s3(target_bucket)
