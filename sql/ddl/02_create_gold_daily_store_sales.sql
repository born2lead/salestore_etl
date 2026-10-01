-- ============================================================================
-- Athena DDL: Gold Zone Daily Store Sales Summary External Table
-- Location: s3://<gold-bucket>/gold/daily_store_sales/
-- Format: Snappy-compressed Apache Parquet partitioned by tx_date
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{ database_name }}.gold_daily_store_sales (
    store_id STRING COMMENT 'Unique store identifier',
    total_transactions BIGINT COMMENT 'Count of distinct transactions',
    unique_customers BIGINT COMMENT 'Count of distinct paying customers',
    total_revenue DOUBLE COMMENT 'Total daily revenue for the store',
    avg_order_value DOUBLE COMMENT 'Average revenue per transaction',
    total_units_sold BIGINT COMMENT 'Total volume of product units sold',
    ingested_at TIMESTAMP COMMENT 'Audit metadata: transformation execution timestamp',
    data_source STRING COMMENT 'Audit metadata: source upstream tier'
)
PARTITIONED BY (
    tx_date STRING COMMENT 'Partition key: transaction date (YYYY-MM-DD)'
)
STORED AS PARQUET
LOCATION 's3://{{ gold_bucket }}/gold/daily_store_sales/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'parquet'
);

-- Note: Run MSCK REPAIR TABLE {{ database_name }}.gold_daily_store_sales to register partitions.
