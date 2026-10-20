-- Both order files go through the same checks. The increment file uses the newest copy in raw_orders,
--  and its future-date cutoff is one day later (it is the next morning's file).

CREATE OR REPLACE TABLE chk_orders AS
WITH src AS (
    SELECT * FROM {{raw}}
    WHERE (_source_file = 'orders.csv' AND _run_id = '{{run_id}}')
       OR (_source_file = 'orders_2026-10-01_increment.csv' AND _run_id = (
            SELECT _run_id FROM {{raw}} WHERE _source_file = 'orders_2026-10-01_increment.csv'
            ORDER BY _loaded_at DESC, _run_id DESC LIMIT 1))
)
, cust_ids AS (SELECT DISTINCT customer_id FROM raw_customers WHERE _run_id = '{{run_id}}')
, prod_ids AS (SELECT DISTINCT product_id FROM raw_products WHERE _run_id = '{{run_id}}')
, store_ids AS (SELECT DISTINCT store_id FROM raw_stores WHERE _run_id = '{{run_id}}')
, prod_keep AS (   -- one catalogue row per product (the non-"(old)" one), used for the price check
    SELECT product_id, TRY_CAST(mrp AS DOUBLE) AS mrp_cat FROM raw_products
    WHERE _run_id = '{{run_id}}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id
                               ORDER BY (product_name LIKE '%(old)%'), _row_num) = 1)
, store_cap AS (
    SELECT store_id, TRY_CAST(capacity_orders_per_day AS DOUBLE) AS cap FROM raw_stores
    WHERE _run_id = '{{run_id}}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _row_num) = 1)
, typed AS (
    SELECT s.*,
        TRY_CAST(s.quantity AS DOUBLE) AS qty, TRY_CAST(s.unit_price AS DOUBLE) AS price,
        TRY_CAST(s.discount_pct AS DOUBLE) AS disc, TRY_CAST(s.order_amount AS DOUBLE) AS amt,
        TRY_CAST(s.delivery_minutes AS DOUBLE) AS mins,
        TRY_STRPTIME(s.order_ts, '%Y-%m-%d %H:%M:%S') AS ts_val,
        UPPER(TRIM(s.order_status)) AS status_u,
        CASE LOWER(TRIM(s.payment_mode))
        WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
        WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
        WHEN 'cashh' THEN 'Cash on Delivery' END AS pay_std,
        ROW_NUMBER() OVER (PARTITION BY s._source_file, s.order_id ORDER BY s._row_num) AS dup_rank,
        s.customer_id IN (SELECT customer_id FROM cust_ids) AS in_customers,
        s.product_id IN (SELECT product_id FROM prod_ids) AS in_products,
        s.store_id IN (SELECT store_id FROM store_ids) AS in_stores,
        pk.mrp_cat, sc.cap,
        COUNT(*) OVER (PARTITION BY s._source_file, s.store_id,
                       CAST(TRY_STRPTIME(s.order_ts, '%Y-%m-%d %H:%M:%S') AS DATE)) AS per_day
    FROM src s
    LEFT JOIN prod_keep pk ON pk.product_id = s.product_id
    LEFT JOIN store_cap sc ON sc.store_id = s.store_id
)
SELECT *,
        COALESCE(TRIM(customer_id) = '', FALSE) AS c_customer_id_not_null,
        COALESCE(TRIM(order_ts) = '', FALSE) AS c_order_ts_not_null,
        COALESCE(status_u = 'DELIVERED' AND TRIM(delivery_minutes) = '', FALSE) AS c_delivered_has_delivery_minutes,
        COALESCE(dup_rank > 1, FALSE) AS c_order_id_unique,
        COALESCE(status_u NOT IN ('DELIVERED','CANCELLED','RETURNED'), FALSE) AS c_order_status_allowed,
        COALESCE(status_u IN ('DELIVERED','CANCELLED','RETURNED') AND order_status <> status_u, FALSE) AS c_order_status_casing,
        COALESCE(pay_std IS NULL, FALSE) AS c_payment_mode_known,
        COALESCE(pay_std IS NOT NULL AND payment_mode <> pay_std, FALSE) AS c_payment_mode_standardised,
        COALESCE(disc IS NULL OR disc < 0 OR disc > 100, FALSE) AS c_discount_pct_between_0_and_100,
        COALESCE(ts_val IS NULL AND TRIM(order_ts) <> '', FALSE) AS c_order_ts_parses,
        COALESCE(qty IS NULL OR qty <= 0, FALSE) AS c_quantity_positive,
        COALESCE(NOT COALESCE(ABS(ROUND(qty * price * (1 - disc / 100), 2) - amt) <= 0.01, FALSE), FALSE) AS c_order_amount_matches_formula,
        COALESCE(mins > 120, FALSE) AS c_delivery_minutes_at_most_120,
        COALESCE(mins <= 0, FALSE) AS c_delivery_minutes_positive,
        COALESCE(mins > 0 AND mins <= 120 AND (mins < 8 OR mins > 35), FALSE) AS c_delivery_minutes_typical_8_to_35,
        COALESCE(mrp_cat IS NOT NULL AND mrp_cat > 0 AND ABS(price - mrp_cat) > 0.01, FALSE) AS c_unit_price_matches_catalogue_mrp,
        COALESCE(TRIM(customer_id) <> '' AND NOT in_customers, FALSE) AS c_customer_exists_in_customers,
        COALESCE(NOT in_products, FALSE) AS c_product_exists_in_products,
        COALESCE(NOT in_stores, FALSE) AS c_store_exists_in_stores,
        COALESCE(cap IS NOT NULL AND cap > 0 AND ts_val IS NOT NULL AND per_day > cap, FALSE) AS c_store_daily_orders_within_capacity,
        COALESCE(ts_val > CASE WHEN _source_file = 'orders_2026-10-01_increment.csv' THEN TIMESTAMP '{{as_of}}' + INTERVAL 1 DAY ELSE TIMESTAMP '{{as_of}}' END, FALSE) AS c_order_ts_not_in_future,
        COALESCE(_source_file = 'orders.csv' AND ts_val < TIMESTAMP '2026-09-01', FALSE) AS c_order_ts_inside_september
FROM typed;
