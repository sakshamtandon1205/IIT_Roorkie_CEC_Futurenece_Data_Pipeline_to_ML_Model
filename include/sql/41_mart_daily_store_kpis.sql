-- Business question 1: daily net revenue by zone and by store (delivered orders only).
-- Also carries the order counts and delivery numbers, so one row = one store on one day.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT f.order_date, COALESCE(s.zone, 'Unknown') AS zone, f.store_id, s.store_name,
       COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
       COUNT(*) FILTER (WHERE f.order_status = 'CANCELLED') AS cancelled_orders,
       COUNT(*) FILTER (WHERE f.order_status = 'RETURNED') AS returned_orders,
       ROUND(SUM(f.net_revenue), 2) AS net_revenue,
       ROUND(SUM(f.gross_margin), 2) AS gross_margin,
       ROUND(AVG(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes
FROM elt_fact_orders f
LEFT JOIN stg_stores s ON s.store_id = f.store_id AND s.dq_status = 'PASSED'
GROUP BY f.order_date, COALESCE(s.zone, 'Unknown'), f.store_id, s.store_name;
