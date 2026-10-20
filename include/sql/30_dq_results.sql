-- One row per check per run: run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at.
--  status is PASS when nothing failed, FAIL for a Critical check that failed, WARN for a Warning check that failed.
--  Includes two whole-file checks: latest order not older than 1 day, and order volume vs the previous run.

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}';

INSERT INTO dq_results
WITH results AS (
    SELECT 'customers.city_not_null' AS check_name, 'Completeness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_city_not_null) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.customer_id_unique' AS check_name, 'Uniqueness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_customer_id_unique) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.email_unique_across_customers' AS check_name, 'Uniqueness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_email_unique_across_customers) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.loyalty_tier_allowed' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_loyalty_tier_allowed) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.email_format_valid' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_email_format_valid) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.phone_is_10_digits' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_phone_is_10_digits) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.pincode_is_6_digits' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_pincode_is_6_digits) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.is_active_is_y_or_n' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_is_active_is_y_or_n) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.signup_date_parses' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_signup_date_parses) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.loyalty_tier_casing_consistent' AS check_name, 'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_loyalty_tier_casing_consistent) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'customers.signup_date_not_in_future' AS check_name, 'Timeliness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_signup_date_not_in_future) AS rows_failed
    FROM chk_customers WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.category_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_category_not_null) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.product_id_unique' AS check_name, 'Uniqueness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_product_id_unique) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.category_is_known' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_category_is_known) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.is_perishable_is_true_or_false' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_is_perishable_is_true_or_false) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.mrp_positive' AS check_name, 'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_mrp_positive) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.cost_price_positive' AS check_name, 'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_cost_price_positive) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.cost_price_not_above_mrp' AS check_name, 'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_cost_price_not_above_mrp) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.no_old_catalogue_rows' AS check_name, 'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_no_old_catalogue_rows) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'products.launch_date_not_in_future' AS check_name, 'Timeliness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_launch_date_not_in_future) AS rows_failed
    FROM chk_products WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.zone_not_null' AS check_name, 'Completeness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_zone_not_null) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.store_id_unique' AS check_name, 'Uniqueness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_store_id_unique) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.zone_is_known' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_zone_is_known) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.pincode_is_6_digits' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_pincode_is_6_digits) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.capacity_positive' AS check_name, 'Accuracy' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_capacity_positive) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'stores.open_date_not_in_future' AS check_name, 'Timeliness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_open_date_not_in_future) AS rows_failed
    FROM chk_stores WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.gateway_ref_present_for_non_cash' AS check_name, 'Completeness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_gateway_ref_present_for_non_cash) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_id_unique' AS check_name, 'Uniqueness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_id_unique) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_status_allowed' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_status_allowed) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_mode_known' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_mode_known) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_mode_standardised' AS check_name, 'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_mode_standardised) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.amount_paid_positive' AS check_name, 'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_amount_paid_positive) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.order_exists_in_orders' AS check_name, 'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_exists_in_orders) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.success_amount_equals_order_amount' AS check_name, 'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_success_amount_equals_order_amount) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.cancelled_order_has_no_payment' AS check_name, 'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_cancelled_order_has_no_payment) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_not_before_order' AS check_name, 'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_not_before_order) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.delivered_order_payment_not_failed' AS check_name, 'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_delivered_order_payment_not_failed) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.success_payment_has_valid_order_status' AS check_name, 'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_success_payment_has_valid_order_status) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT 'payments.payment_ts_not_in_future' AS check_name, 'Timeliness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_ts_not_in_future) AS rows_failed
    FROM chk_payments WHERE _run_id = '{{run_id}}'
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.customer_id_not_null' AS check_name,
           'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_customer_id_not_null) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_ts_not_null' AS check_name,
           'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_ts_not_null) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.delivered_has_delivery_minutes' AS check_name,
           'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_delivered_has_delivery_minutes) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_id_unique' AS check_name,
           'Uniqueness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_id_unique) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_status_allowed' AS check_name,
           'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_status_allowed) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_status_casing' AS check_name,
           'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_status_casing) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.payment_mode_known' AS check_name,
           'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_mode_known) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.payment_mode_standardised' AS check_name,
           'Validity' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_payment_mode_standardised) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.discount_pct_between_0_and_100' AS check_name,
           'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_discount_pct_between_0_and_100) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_ts_parses' AS check_name,
           'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_ts_parses) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.quantity_positive' AS check_name,
           'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_quantity_positive) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_amount_matches_formula' AS check_name,
           'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_amount_matches_formula) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.delivery_minutes_at_most_120' AS check_name,
           'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_delivery_minutes_at_most_120) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.delivery_minutes_positive' AS check_name,
           'Accuracy' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_delivery_minutes_positive) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.delivery_minutes_typical_8_to_35' AS check_name,
           'Accuracy' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_delivery_minutes_typical_8_to_35) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.unit_price_matches_catalogue_mrp' AS check_name,
           'Accuracy' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_unit_price_matches_catalogue_mrp) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.customer_exists_in_customers' AS check_name,
           'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_customer_exists_in_customers) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.product_exists_in_products' AS check_name,
           'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_product_exists_in_products) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.store_exists_in_stores' AS check_name,
           'Consistency' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_store_exists_in_stores) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.store_daily_orders_within_capacity' AS check_name,
           'Consistency' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_store_daily_orders_within_capacity) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_ts_not_in_future' AS check_name,
           'Timeliness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_ts_not_in_future) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}'
    GROUP BY _source_file
UNION ALL
    SELECT CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'increment' END || '.order_ts_inside_september' AS check_name,
           'Timeliness' AS dimension, 'Warning' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE c_order_ts_inside_september) AS rows_failed
    FROM chk_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
    GROUP BY _source_file
UNION ALL
    SELECT 'orders.latest_order_within_1_day', 'Timeliness', 'Warning', 1,
           CASE WHEN MAX(ts_val) IS NULL OR TIMESTAMP '{{as_of}}' - MAX(ts_val) > INTERVAL 1 DAY THEN 1 ELSE 0 END
    FROM chk_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
UNION ALL
    SELECT 'orders.volume_vs_last_run', 'Completeness', 'Warning', COUNT(*),
           CASE WHEN (SELECT rows_checked FROM dq_results
                      WHERE dag_id = '{{dag_id}}' AND check_name = 'orders.volume_vs_last_run'
                        AND run_id <> '{{run_id}}'
                      ORDER BY checked_at DESC LIMIT 1) * 0.10 > COUNT(*) THEN 1 ELSE 0 END
    FROM chk_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
)
SELECT '{{run_id}}', '{{dag_id}}', check_name, dimension, severity, rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMESTAMP '{{now}}'
FROM results;
