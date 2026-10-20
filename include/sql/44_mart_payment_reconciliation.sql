-- Business question 4 (second half): every order/payment mismatch, one row each, with a reason.
-- Payments are read from raw_payments (de-duplicated by payment_id, first row wins) so that a
-- payment rejected from staging still shows up here. Orders come from the passed orders only.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH pay AS (
    SELECT payment_id, order_id, payment_status, TRY_CAST(amount_paid AS DOUBLE) AS amount, _row_num
    FROM raw_payments WHERE _run_id = '{{run_id}}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _row_num) = 1),
succ AS (SELECT order_id, SUM(amount) AS paid, ARG_MIN(payment_id, _row_num) AS payment_id
         FROM pay WHERE payment_status = 'SUCCESS' GROUP BY order_id),
fail AS (SELECT order_id, ARG_MIN(payment_id, _row_num) AS payment_id
         FROM pay WHERE payment_status = 'FAILED' GROUP BY order_id),
refunded AS (SELECT DISTINCT order_id FROM pay WHERE payment_status = 'REFUNDED'),
any_pay AS (SELECT DISTINCT order_id FROM pay),
raw_order_ids AS (SELECT DISTINCT order_id FROM raw_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'),
mismatch AS (
    SELECT f.order_id, NULL AS payment_id, 'DELIVERED_NO_PAYMENT' AS mismatch_type, 'DELIVERED' AS order_status,
           CAST(NULL AS VARCHAR) AS payment_status, f.order_amount, 0.0 AS amount_paid,
           'Delivered order has no payment row in the gateway export' AS reason
    FROM elt_fact_orders f WHERE f.order_status = 'DELIVERED' AND f.order_id NOT IN (SELECT order_id FROM any_pay)
  UNION ALL
    SELECT f.order_id, fl.payment_id, 'DELIVERED_PAYMENT_FAILED', 'DELIVERED', 'FAILED', f.order_amount, 0.0,
           'Delivered order, but the only payment attempt FAILED'
    FROM elt_fact_orders f LEFT JOIN fail fl ON fl.order_id = f.order_id
    WHERE f.order_status = 'DELIVERED' AND f.order_id IN (SELECT order_id FROM any_pay)
      AND f.order_id NOT IN (SELECT order_id FROM succ)
  UNION ALL
    SELECT f.order_id, s.payment_id, 'AMOUNT_MISMATCH', 'DELIVERED', 'SUCCESS', f.order_amount, s.paid,
           'SUCCESS payment differs from order_amount'
    FROM elt_fact_orders f JOIN succ s ON s.order_id = f.order_id
    WHERE f.order_status = 'DELIVERED' AND ABS(COALESCE(s.paid, 0) - f.order_amount) > 0.01
  UNION ALL
    SELECT f.order_id, s.payment_id, 'CANCELLED_HAS_PAYMENT', 'CANCELLED', 'SUCCESS', f.order_amount, COALESCE(s.paid, 0),
           'Cancelled order still has a SUCCESS payment'
    FROM elt_fact_orders f JOIN succ s ON s.order_id = f.order_id WHERE f.order_status = 'CANCELLED'
  UNION ALL
    SELECT f.order_id, s.payment_id, 'RETURNED_NOT_REFUNDED', 'RETURNED', 'SUCCESS', f.order_amount, COALESCE(s.paid, 0),
           'Returned order was paid but no refund is recorded'
    FROM elt_fact_orders f JOIN succ s ON s.order_id = f.order_id
    WHERE f.order_status = 'RETURNED' AND f.order_id NOT IN (SELECT order_id FROM refunded)
  UNION ALL
    SELECT p.order_id, p.payment_id, 'PAYMENT_WITHOUT_ORDER', NULL, p.payment_status, NULL, COALESCE(p.amount, 0),
           CASE WHEN p.order_id NOT IN (SELECT order_id FROM raw_order_ids)
                THEN 'Order is not in the orders file'
                ELSE 'Order was quarantined, so it is not in the fact table' END
    FROM pay p WHERE p.order_id NOT IN (SELECT order_id FROM elt_fact_orders))
SELECT '{{run_id}}' AS run_id, order_id, payment_id, mismatch_type, order_status, payment_status,
       order_amount, amount_paid, ROUND(COALESCE(order_amount, 0) - COALESCE(amount_paid, 0), 2) AS difference, reason
FROM mismatch;
