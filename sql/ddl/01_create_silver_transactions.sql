-- ============================================================================
-- Athena DDL: Silver Zone Cleansed Transactions External Table
-- Location: s3://<silver-bucket>/silver/transactions/
-- Format: Snappy-compressed Apache Parquet partitioned by tx_date
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{ database_name }}.silver_transactions (
    transaction_id STRING COMMENT 'Unique transaction business key',
    customer_id STRING COMMENT 'Customer unique identifier',
    store_id STRING COMMENT 'Store location identifier',
    product_id STRING COMMENT 'Product stock keeping unit',
    quantity INT COMMENT 'Number of units purchased',
    unit_price DOUBLE COMMENT 'Unit price per product item',
    total_amount DOUBLE COMMENT 'Calculated or verified total transaction price',
    payment_method STRING COMMENT 'Payment method (CARD, CASH, UPI, UNKNOWN)',
    tx_timestamp TIMESTAMP COMMENT 'Timestamp of transaction execution',
    calculated_amount DOUBLE COMMENT 'Computed quantity * unit_price',
    ingested_at TIMESTAMP COMMENT 'Audit metadata: ingestion timestamp',
    data_source STRING COMMENT 'Audit metadata: source system'
)
PARTITIONED BY (
    tx_date STRING COMMENT 'Partition key: transaction date (YYYY-MM-DD)'
)
STORED AS PARQUET
LOCATION 's3://{{ silver_bucket }}/silver/transactions/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'parquet'
);

-- Note: Run MSCK REPAIR TABLE {{ database_name }}.silver_transactions to register partitions.
