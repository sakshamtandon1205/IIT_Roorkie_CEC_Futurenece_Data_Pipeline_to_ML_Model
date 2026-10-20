-- Totals must match the ETL pipeline. One row per metric; status MATCH or DIFFERENT.
CREATE OR REPLACE TABLE elt_vs_etl_totals AS
WITH etl AS (
    SELECT 'fact_rows' AS metric, COUNT(*)::DOUBLE AS v FROM etl_fact_orders
    UNION ALL SELECT 'net_revenue', ROUND(SUM(net_revenue), 2) FROM etl_fact_orders
    UNION ALL SELECT 'gross_margin', ROUND(SUM(gross_margin), 2) FROM etl_fact_orders
    UNION ALL SELECT 'delivered_orders', COUNT(*) FILTER (WHERE order_status = 'DELIVERED')::DOUBLE FROM etl_fact_orders
    UNION ALL SELECT 'reconciliation_mismatches', COUNT(*)::DOUBLE FROM etl_payment_reconciliation),
elt AS (
    SELECT 'fact_rows' AS metric, COUNT(*)::DOUBLE AS v FROM elt_fact_orders
    UNION ALL SELECT 'net_revenue', ROUND(SUM(net_revenue), 2) FROM elt_fact_orders
    UNION ALL SELECT 'gross_margin', ROUND(SUM(gross_margin), 2) FROM elt_fact_orders
    UNION ALL SELECT 'delivered_orders', COUNT(*) FILTER (WHERE order_status = 'DELIVERED')::DOUBLE FROM elt_fact_orders
    UNION ALL SELECT 'reconciliation_mismatches', COUNT(*)::DOUBLE FROM mart_payment_reconciliation)
SELECT etl.metric, etl.v AS etl_value, elt.v AS elt_value, ROUND(elt.v - etl.v, 2) AS difference,
       CASE WHEN ABS(elt.v - etl.v) < 0.01 THEN 'MATCH' ELSE 'DIFFERENT' END AS status
FROM etl JOIN elt USING (metric);
