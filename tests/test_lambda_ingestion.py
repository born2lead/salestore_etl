"""
Unit Tests for AWS Lambda Raw Data Ingestion.
Tests payload parsing, S3 put_object invocation, key structure, and error handling.
"""

import os
import json
from unittest.mock import patch, MagicMock

# Set dummy AWS credentials before import to avoid credential provider lookup
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

from src.ingestion.lambda_raw_ingest import lambda_handler


@patch("src.ingestion.lambda_raw_ingest.get_s3_client")
def test_lambda_handler_direct_records(mock_get_s3):
    mock_s3 = MagicMock()
    mock_s3.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_get_s3.return_value = mock_s3
    
    event = {
        "records": [
            {
                "transaction_id": "TX_9999",
                "customer_id": "CUST_100",
                "store_id": "STORE_01",
                "product_id": "PROD_A",
                "quantity": 2,
                "unit_price": 45.0,
                "transaction_timestamp": "2026-09-28T10:00:00Z"
            }
        ]
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["record_count"] == 1
    assert "s3_path" in body
    assert "raw/transactions/year=" in body["s3_path"]
    
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "raw/transactions/year=" in call_kwargs["Key"]
    assert call_kwargs["ContentType"] == "application/json"


@patch("src.ingestion.lambda_raw_ingest.get_s3_client")
def test_lambda_handler_api_gateway_body(mock_get_s3):
    mock_s3 = MagicMock()
    mock_s3.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_get_s3.return_value = mock_s3
    
    payload_body = [
        {"transaction_id": "TX_01", "total_amount": 100.0},
        {"transaction_id": "TX_02", "total_amount": 200.0}
    ]
    event = {
        "body": json.dumps(payload_body)
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["record_count"] == 2
    mock_s3.put_object.assert_called_once()


@patch("src.ingestion.lambda_raw_ingest.get_s3_client")
def test_lambda_handler_s3_error(mock_get_s3):
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("S3 Service Unavailable")
    mock_get_s3.return_value = mock_s3
    
    event = {"records": [{"transaction_id": "TX_FAIL"}]}
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    assert "error" in body
    assert "S3 Service Unavailable" in body["error"]
