-- Business question 4 (first half): revenue by payment mode.
CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT payment_mode, ROUND(SUM(net_revenue), 2) AS revenue,
       COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders
FROM elt_fact_orders GROUP BY payment_mode;
