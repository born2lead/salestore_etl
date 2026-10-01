"""
AWS Lambda Function: Raw Data Ingestion.
Extracts incoming batch payloads or API payloads, validates basic JSON structure,
and writes raw immutable landing files to S3 Raw Bucket (Raw Zone).
"""

import os
import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME', 'aws-de-lakehouse-raw-zone')


def get_s3_client():
    return boto3.client('s3')


def lambda_handler(event, context):
    """
    Lambda Handler triggered by API Gateway, EventBridge, or S3 Put Event.
    """
    logger.info(f"Received ingestion event: {json.dumps(event)}")
    
    try:
        # Extract payload from event
        if 'body' in event:
            payload = json.loads(event['body'])
        else:
            payload = event.get('records', [event])

        now = datetime.now(timezone.utc)
        date_prefix = now.strftime("year=%Y/month=%m/day=%d")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")
        
        file_key = f"raw/transactions/{date_prefix}/transaction_batch_{timestamp_str}.json"
        
        # Write to S3 Raw Bucket
        s3 = get_s3_client()
        s3.put_object(
            Bucket=RAW_BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(payload),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully ingested raw landing file to s3://{RAW_BUCKET_NAME}/{file_key}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Raw data ingestion successful",
                "s3_path": f"s3://{RAW_BUCKET_NAME}/{file_key}",
                "record_count": len(payload) if isinstance(payload, list) else 1
            })
        }
        
    except Exception as e:
        logger.error(f"Error during raw ingestion: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
