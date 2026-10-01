"""
Unit Tests for AWS Boto3 Helpers.
Tests Glue crawler triggers and SNS alerts with mock clients.
"""

import os
from unittest.mock import patch, MagicMock

os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

from src.utils.aws_helpers import trigger_glue_crawler, send_sns_notification


@patch("src.utils.aws_helpers.get_boto3_client")
def test_trigger_glue_crawler_success(mock_get_client):
    mock_glue = MagicMock()
    mock_glue.start_crawler.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_get_client.return_value = mock_glue
    
    status = trigger_glue_crawler("my-crawler", "us-east-1")
    assert status == 200
    mock_glue.start_crawler.assert_called_once_with(Name="my-crawler")


@patch("src.utils.aws_helpers.get_boto3_client")
def test_trigger_glue_crawler_already_running(mock_get_client):
    mock_glue = MagicMock()
    # Mock CrawlerRunningException
    class MockCrawlerRunningException(Exception):
        pass
        
    mock_glue.exceptions.CrawlerRunningException = MockCrawlerRunningException
    mock_glue.start_crawler.side_effect = MockCrawlerRunningException("Crawler is running")
    mock_get_client.return_value = mock_glue
    
    status = trigger_glue_crawler("my-running-crawler")
    assert status == "ALREADY_RUNNING"


@patch("src.utils.aws_helpers.get_boto3_client")
def test_send_sns_notification_success(mock_get_client):
    mock_sns = MagicMock()
    mock_sns.publish.return_value = {"MessageId": "msg-12345-abc"}
    mock_get_client.return_value = mock_sns
    
    msg_id = send_sns_notification(
        topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
        subject="Pipeline Alert",
        message_dict={"status": "SUCCESS", "records": 100}
    )
    
    assert msg_id == "msg-12345-abc"
    mock_sns.publish.assert_called_once()
    kwargs = mock_sns.publish.call_args[1]
    assert kwargs["Subject"] == "Pipeline Alert"
    assert '"status": "SUCCESS"' in kwargs["Message"]


def test_send_sns_notification_missing_arn():
    # Should safely return empty string if no ARN provided
    res = send_sns_notification(topic_arn="", subject="Test", message_dict={})
    assert res == ""
