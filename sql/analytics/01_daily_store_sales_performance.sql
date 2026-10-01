-- ============================================================================
-- Athena Analytics Query: Daily Store Sales Performance & Growth
-- Business Goal: Identify top-performing stores, revenue trends, and basket sizes
-- Target Table: gold_daily_store_sales
-- ============================================================================

WITH store_daily_ranked AS (
    SELECT
        store_id,
        tx_date,
        total_revenue,
        total_transactions,
        unique_customers,
        avg_order_value,
        total_units_sold,
        -- Running 7-day average revenue per store
        AVG(total_revenue) OVER (
            PARTITION BY store_id 
            ORDER BY tx_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg_revenue,
        -- Rank stores by daily revenue
        DENSE_RANK() OVER (
            PARTITION BY tx_date 
            ORDER BY total_revenue DESC
        ) AS daily_revenue_rank
    FROM
        gold_daily_store_sales
)
SELECT
    store_id,
    tx_date,
    daily_revenue_rank,
    ROUND(total_revenue, 2) AS total_revenue_usd,
    total_transactions,
    unique_customers,
    ROUND(avg_order_value, 2) AS avg_order_value_usd,
    total_units_sold,
    ROUND(rolling_7d_avg_revenue, 2) AS rolling_7d_avg_revenue_usd
FROM
    store_daily_ranked
ORDER BY
    tx_date DESC,
    daily_revenue_rank ASC;
