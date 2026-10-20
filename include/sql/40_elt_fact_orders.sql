-- A view that every mart reads from: passed orders with revenue, margin and the 15-minute flag.
--   net_revenue   = order_amount for DELIVERED orders, 0 otherwise (amount is already after discount)
--   gross_margin  = net_revenue - quantity x cost_price (empty when the product's cost is unknown)
--   within_15_min = DELIVERED and delivery_minutes <= 15
-- Only rows with dq_status = 'PASSED' are here, so rejected orders never reach a mart.
CREATE OR REPLACE VIEW elt_fact_orders AS
SELECT o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id, o.quantity,
       o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status, o.channel,
       o.delivery_minutes,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS net_revenue,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END AS gross_margin,
       o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 AS within_15_min,
       o._source_file, o._run_id, o._row_num
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED';
