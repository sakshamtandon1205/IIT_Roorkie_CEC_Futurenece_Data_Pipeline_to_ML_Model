-- Business question 5: top 10 customers by spend, with loyalty tier. Only masked columns are used.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT f.customer_id, c.customer_name_masked, c.loyalty_tier,
       ROUND(SUM(f.net_revenue), 2) AS total_spend,
       COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders
FROM elt_fact_orders f
LEFT JOIN stg_customers c ON c.customer_id = f.customer_id AND c.dq_status = 'PASSED'
GROUP BY f.customer_id, c.customer_name_masked, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
