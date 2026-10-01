"""
AWS Boto3 Helper Utilities.
Encapsulates S3 operations, AWS Glue Crawler execution, and SNS notification alerts.
"""

import boto3
import logging
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)

def get_boto3_client(service_name: str, region_name: str = "us-east-1"):
    """
    Returns an authenticated Boto3 client for a given AWS service.
    """
    return boto3.client(service_name, region_name=region_name)


def trigger_glue_crawler(crawler_name: str, region_name: str = "us-east-1") -> str:
    """
    Triggers an AWS Glue Crawler to scan updated S3 partitions and update Athena catalog metadata.
    """
    glue_client = get_boto3_client("glue", region_name)
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Triggered Glue Crawler '{crawler_name}' successfully.")
        return response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    except glue_client.exceptions.CrawlerRunningException:
        logger.warning(f"Glue Crawler '{crawler_name}' is already running.")
        return "ALREADY_RUNNING"
    except Exception as e:
        logger.error(f"Failed to start Glue Crawler '{crawler_name}': {str(e)}")
        raise e


def send_sns_notification(
    topic_arn: str,
    subject: str,
    message_dict: Dict[str, Any],
    region_name: str = "us-east-1"
) -> str:
    """
    Sends structured JSON notification to AWS SNS topic (alerts on pipeline status).
    """
    if not topic_arn:
        logger.warning("SNS Topic ARN not provided. Skipping notification.")
        return ""
        
    sns_client = get_boto3_client("sns", region_name)
    formatted_msg = json.dumps(message_dict, indent=2)
    
    try:
        res = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=formatted_msg
        )
        msg_id = res.get("MessageId")
        logger.info(f"Sent SNS alert to topic {topic_arn} with MessageId: {msg_id}")
        return msg_id
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {str(e)}")
        return ""
