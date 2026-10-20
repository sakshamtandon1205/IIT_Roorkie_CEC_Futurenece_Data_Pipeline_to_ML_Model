-- The six business questions, one simple query each on the ELT marts (used by the report task).
-- Q1 daily net revenue by zone and store
SELECT order_date, zone, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id LIMIT 12;
-- Q2 revenue and gross margin by category
SELECT * FROM mart_category_margin ORDER BY revenue DESC;
-- Q3 delivery time and % within 15 minutes per store
SELECT * FROM mart_delivery_sla ORDER BY store_id;
-- Q4 revenue by payment mode, and the amount that does not reconcile
SELECT * FROM mart_revenue_by_payment_mode ORDER BY revenue DESC;
SELECT ROUND(SUM(ABS(difference)), 2) AS amount_not_reconciling, COUNT(*) AS mismatches FROM mart_payment_reconciliation;
-- Q5 top 10 customers (masked)
SELECT * FROM mart_top_customers;
-- Q6 rows rejected in this run, by file and reason
SELECT file, failure_reason, COUNT(*) AS rows FROM elt_rejected_rows WHERE _run_id = '{{run_id}}' GROUP BY 1, 2 ORDER BY 1, 3 DESC;
