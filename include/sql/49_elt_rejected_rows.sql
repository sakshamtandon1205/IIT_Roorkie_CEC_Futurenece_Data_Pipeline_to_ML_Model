-- Every row rejected by staging, one line per failed rule (Q6). _row_num points back to the exact raw row.
CREATE OR REPLACE VIEW elt_rejected_rows AS
SELECT 'orders' AS file, order_id AS row_key, UNNEST(STRING_SPLIT(failure_reason, '; ')) AS failure_reason, _source_file, _run_id, _row_num
FROM stg_orders WHERE dq_status = 'QUARANTINED'
UNION ALL
SELECT 'payments', payment_id, UNNEST(STRING_SPLIT(failure_reason, '; ')), _source_file, _run_id, _row_num
FROM stg_payments WHERE dq_status = 'QUARANTINED'
UNION ALL
SELECT 'customers', customer_id, UNNEST(STRING_SPLIT(failure_reason, '; ')), _source_file, _run_id, _row_num
FROM stg_customers WHERE dq_status = 'QUARANTINED'
UNION ALL
SELECT 'products', product_id, UNNEST(STRING_SPLIT(failure_reason, '; ')), _source_file, _run_id, _row_num
FROM stg_products WHERE dq_status = 'QUARANTINED'
UNION ALL
SELECT 'stores', store_id, UNNEST(STRING_SPLIT(failure_reason, '; ')), _source_file, _run_id, _row_num
FROM stg_stores WHERE dq_status = 'QUARANTINED';
