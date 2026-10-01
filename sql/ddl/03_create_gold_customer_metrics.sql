-- ============================================================================
-- Athena DDL: Gold Zone Customer 360 RFM Metrics External Table
-- Location: s3://<gold-bucket>/gold/customer_metrics/
-- Format: Snappy-compressed Apache Parquet (Non-partitioned / Full Table Snapshot)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{ database_name }}.gold_customer_metrics (
    customer_id STRING COMMENT 'Unique customer identifier',
    total_orders BIGINT COMMENT 'Lifetime total transaction count',
    lifetime_spend DOUBLE COMMENT 'Aggregated cumulative gross spend in USD',
    avg_basket_value DOUBLE COMMENT 'Mean spend per order',
    last_order_date DATE COMMENT 'Recency indicator: most recent transaction date',
    first_order_date DATE COMMENT 'Customer acquisition date: initial transaction date',
    ingested_at TIMESTAMP COMMENT 'Audit metadata: transformation execution timestamp',
    data_source STRING COMMENT 'Audit metadata: source upstream tier'
)
STORED AS PARQUET
LOCATION 's3://{{ gold_bucket }}/gold/customer_metrics/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'parquet'
);
