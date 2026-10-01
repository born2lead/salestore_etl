-- ============================================================================
-- Athena Analytics Query: Customer 360 RFM Segmentation
-- Business Goal: Classify customer base into actionable loyalty tiers
-- Target Table: gold_customer_metrics
-- ============================================================================

WITH customer_rfm_base AS (
    SELECT
        customer_id,
        total_orders,
        lifetime_spend,
        avg_basket_value,
        first_order_date,
        last_order_date,
        -- Calculate Recency (days since last transaction relative to current date)
        DATE_DIFF('day', last_order_date, CURRENT_DATE) AS days_since_last_order,
        -- Calculate Customer Tenure (days since initial purchase)
        DATE_DIFF('day', first_order_date, CURRENT_DATE) AS customer_tenure_days
    FROM
        gold_customer_metrics
),
rfm_scores AS (
    SELECT
        customer_id,
        total_orders,
        lifetime_spend,
        avg_basket_value,
        days_since_last_order,
        customer_tenure_days,
        -- NTILE scoring (1 to 4 scale: 4 is highest/best)
        NTILE(4) OVER (ORDER BY days_since_last_order DESC) AS recency_score,
        NTILE(4) OVER (ORDER BY total_orders ASC) AS frequency_score,
        NTILE(4) OVER (ORDER BY lifetime_spend ASC) AS monetary_score
    FROM
        customer_rfm_base
)
SELECT
    customer_id,
    total_orders,
    ROUND(lifetime_spend, 2) AS lifetime_spend_usd,
    ROUND(avg_basket_value, 2) AS avg_basket_value_usd,
    days_since_last_order,
    customer_tenure_days,
    recency_score,
    frequency_score,
    monetary_score,
    -- Customer Behavioral Segmentation Classification
    CASE
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'CHAMPION'
        WHEN recency_score >= 3 AND frequency_score >= 2 THEN 'LOYAL_CUSTOMER'
        WHEN recency_score >= 3 AND frequency_score = 1 THEN 'RECENT_NEW_CUSTOMER'
        WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'AT_RISK_HIGH_VALUE'
        WHEN recency_score = 1 AND frequency_score <= 2 THEN 'HIBERNATING_OR_LOST'
        ELSE 'STANDARD_ACTIVE'
    END AS customer_segment
FROM
    rfm_scores
ORDER BY
    lifetime_spend DESC;
