-- Passed increment rows: a matching order_id is UPDATED (the 10 DELIVERED -> RETURNED corrections),
--  a new order_id is INSERTED (the 150 new orders). Rows that failed a check never overwrite good data;
--  they are added as QUARANTINED rows so they stay visible. Safe to run twice: the result does not change.

MERGE INTO stg_orders AS t
USING (SELECT * FROM stg_orders_increment WHERE dq_status = 'PASSED') AS s
ON t.order_id = s.order_id AND t.dq_status = 'PASSED'
WHEN MATCHED THEN UPDATE SET
    order_ts = s.order_ts,
    order_date = s.order_date,
    customer_id = s.customer_id,
    store_id = s.store_id,
    product_id = s.product_id,
    quantity = s.quantity,
    unit_price = s.unit_price,
    discount_pct = s.discount_pct,
    order_amount = s.order_amount,
    payment_mode = s.payment_mode,
    order_status = s.order_status,
    channel = s.channel,
    delivery_minutes = s.delivery_minutes,
    dq_status = s.dq_status,
    failure_reason = s.failure_reason,
    _source_file = s._source_file,
    _run_id = s._run_id,
    _row_num = s._row_num,
    _loaded_at = s._loaded_at
WHEN NOT MATCHED THEN INSERT (order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status, failure_reason, _source_file, _run_id, _row_num, _loaded_at)
    VALUES (s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id, s.quantity, s.unit_price, s.discount_pct, s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes, s.dq_status, s.failure_reason, s._source_file, s._run_id, s._row_num, s._loaded_at);

DELETE FROM stg_orders WHERE dq_status = 'QUARANTINED' AND _source_file = 'orders_2026-10-01_increment.csv';

INSERT INTO stg_orders (order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status, failure_reason, _source_file, _run_id, _row_num, _loaded_at)
SELECT order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status, failure_reason, _source_file, _run_id, _row_num, _loaded_at FROM stg_orders_increment WHERE dq_status = 'QUARANTINED';
