-- SQL-only staging step. Rows are never dropped here: rows that broke a Critical check
--  stay in the table with dq_status = 'QUARANTINED' and the reasons in failure_reason.

CREATE OR REPLACE TABLE stg_payments AS
SELECT payment_id, order_id, ts_val AS payment_ts, pay_std AS payment_mode, amt AS amount_paid,
       payment_status, gateway_ref,
       CASE WHEN CONCAT_WS('; ',
        CASE WHEN c_payment_id_unique THEN 'payment_id_unique' END,
        CASE WHEN c_payment_status_allowed THEN 'payment_status_allowed' END,
        CASE WHEN c_payment_mode_known THEN 'payment_mode_known' END,
        CASE WHEN c_amount_paid_positive THEN 'amount_paid_positive' END,
        CASE WHEN c_order_exists_in_orders THEN 'order_exists_in_orders' END,
        CASE WHEN c_success_amount_equals_order_amount THEN 'success_amount_equals_order_amount' END,
        CASE WHEN c_cancelled_order_has_no_payment THEN 'cancelled_order_has_no_payment' END,
        CASE WHEN c_payment_ts_not_in_future THEN 'payment_ts_not_in_future' END) = '' THEN 'PASSED' ELSE 'QUARANTINED' END AS dq_status,
       CONCAT_WS('; ',
        CASE WHEN c_payment_id_unique THEN 'payment_id_unique' END,
        CASE WHEN c_payment_status_allowed THEN 'payment_status_allowed' END,
        CASE WHEN c_payment_mode_known THEN 'payment_mode_known' END,
        CASE WHEN c_amount_paid_positive THEN 'amount_paid_positive' END,
        CASE WHEN c_order_exists_in_orders THEN 'order_exists_in_orders' END,
        CASE WHEN c_success_amount_equals_order_amount THEN 'success_amount_equals_order_amount' END,
        CASE WHEN c_cancelled_order_has_no_payment THEN 'cancelled_order_has_no_payment' END,
        CASE WHEN c_payment_ts_not_in_future THEN 'payment_ts_not_in_future' END) AS failure_reason,
       _source_file, _run_id, _row_num, _loaded_at
FROM chk_payments;
