-- ============================================================================
-- Athena Analytics Query: Data Quality & Medallion Reconciliation Audit
-- Business Goal: Reconcile record counts and revenue between Silver and Gold tiers
-- ============================================================================

WITH silver_summary AS (
    SELECT
        tx_date,
        COUNT(DISTINCT transaction_id) AS silver_tx_count,
        COUNT(DISTINCT customer_id) AS silver_unique_customers,
        ROUND(SUM(total_amount), 2) AS silver_total_revenue,
        SUM(quantity) AS silver_total_units
    FROM
        silver_transactions
    GROUP BY
        tx_date
),
gold_summary AS (
    SELECT
        tx_date,
        SUM(total_transactions) AS gold_tx_count,
        SUM(unique_customers) AS gold_unique_customers_sum,
        ROUND(SUM(total_revenue), 2) AS gold_total_revenue,
        SUM(total_units_sold) AS gold_total_units
    FROM
        gold_daily_store_sales
    GROUP BY
        tx_date
)
SELECT
    COALESCE(s.tx_date, g.tx_date) AS audit_tx_date,
    s.silver_tx_count,
    g.gold_tx_count,
    (s.silver_tx_count - g.gold_tx_count) AS tx_count_delta,
    s.silver_total_revenue,
    g.gold_total_revenue,
    ROUND(s.silver_total_revenue - g.gold_total_revenue, 2) AS revenue_delta,
    CASE 
        WHEN s.silver_tx_count = g.gold_tx_count 
         AND ABS(s.silver_total_revenue - g.gold_total_revenue) < 0.01 
        THEN 'PASSED'
        ELSE 'RECONCILIATION_MISMATCH'
    END AS audit_status
FROM
    silver_summary s
FULL OUTER JOIN
    gold_summary g ON s.tx_date = g.tx_date
ORDER BY
    audit_tx_date DESC;
