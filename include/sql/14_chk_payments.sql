-- Payments are checked against the orders of orders.csv (the same reference the ETL pipeline uses).

CREATE OR REPLACE TABLE chk_payments AS
WITH src AS (
    SELECT * FROM {{raw}}
    WHERE _run_id = '{{run_id}}'
)
, oref AS (   -- first row per order_id of orders.csv, exactly as received
    SELECT * FROM raw_orders
    WHERE _source_file = 'orders.csv' AND _run_id = '{{run_id}}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _row_num) = 1)
, typed AS (
    SELECT s.*,
        TRY_CAST(s.amount_paid AS DOUBLE) AS amt,
        TRY_STRPTIME(s.payment_ts, '%Y-%m-%d %H:%M:%S') AS ts_val,
        CASE LOWER(TRIM(s.payment_mode))
        WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
        WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
        WHEN 'cashh' THEN 'Cash on Delivery' END AS pay_std,
        ROW_NUMBER() OVER (PARTITION BY s.payment_id ORDER BY s._row_num) AS dup_rank,
        o.order_id IS NOT NULL AS known_order,
        TRY_CAST(o.order_amount AS DOUBLE) AS o_amt,
        UPPER(TRIM(COALESCE(o.order_status, ''))) AS o_status,
        TRY_STRPTIME(COALESCE(o.order_ts, ''), '%Y-%m-%d %H:%M:%S') AS o_ts
    FROM src s
    LEFT JOIN oref o ON o.order_id = s.order_id
)
SELECT *,
        COALESCE(TRIM(gateway_ref) = '' AND pay_std IS DISTINCT FROM 'Cash on Delivery', FALSE) AS c_gateway_ref_present_for_non_cash,
        COALESCE(dup_rank > 1, FALSE) AS c_payment_id_unique,
        COALESCE(payment_status NOT IN ('SUCCESS','FAILED','REFUNDED'), FALSE) AS c_payment_status_allowed,
        COALESCE(pay_std IS NULL, FALSE) AS c_payment_mode_known,
        COALESCE(pay_std IS NOT NULL AND payment_mode <> pay_std, FALSE) AS c_payment_mode_standardised,
        COALESCE(amt IS NULL OR amt <= 0, FALSE) AS c_amount_paid_positive,
        COALESCE(NOT known_order, FALSE) AS c_order_exists_in_orders,
        COALESCE(payment_status = 'SUCCESS' AND known_order AND o_amt IS NOT NULL AND NOT COALESCE(ABS(amt - o_amt) <= 0.01, FALSE), FALSE) AS c_success_amount_equals_order_amount,
        COALESCE(known_order AND o_status = 'CANCELLED' AND payment_status = 'SUCCESS', FALSE) AS c_cancelled_order_has_no_payment,
        COALESCE(o_ts IS NOT NULL AND ts_val < o_ts, FALSE) AS c_payment_not_before_order,
        COALESCE(payment_status = 'FAILED' AND o_status = 'DELIVERED', FALSE) AS c_delivered_order_payment_not_failed,
        COALESCE(payment_status = 'SUCCESS' AND known_order AND o_status NOT IN ('DELIVERED','CANCELLED','RETURNED'), FALSE) AS c_success_payment_has_valid_order_status,
        COALESCE(ts_val > TIMESTAMP '{{as_of}}', FALSE) AS c_payment_ts_not_in_future
FROM typed;
