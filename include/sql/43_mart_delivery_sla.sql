-- Business question 3: average delivery time per store and the % of delivered orders within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT f.store_id, s.store_name,
       ROUND(AVG(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
       COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.within_15_min)
             / NULLIF(COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED'), 0), 1) AS pct_within_15_min
FROM elt_fact_orders f
LEFT JOIN stg_stores s ON s.store_id = f.store_id AND s.dq_status = 'PASSED'
GROUP BY f.store_id, s.store_name;
