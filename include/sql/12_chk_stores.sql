-- A blank zone or a zero capacity is a Warning: the store still loads (quarantining it would drop all its orders).

CREATE OR REPLACE TABLE chk_stores AS
WITH src AS (
    SELECT * FROM {{raw}}
    WHERE _run_id = '{{run_id}}'
)
, typed AS (
    SELECT src.*, TRY_CAST(capacity_orders_per_day AS DOUBLE) AS cap,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _row_num) AS dup_rank
    FROM src
)
SELECT *,
        COALESCE(TRIM(zone) = '', FALSE) AS c_zone_not_null,
        COALESCE(dup_rank > 1, FALSE) AS c_store_id_unique,
        COALESCE(TRIM(zone) <> '' AND zone NOT IN ('North','South','East','West'), FALSE) AS c_zone_is_known,
        COALESCE(NOT REGEXP_FULL_MATCH(pincode, '\d{6}'), FALSE) AS c_pincode_is_6_digits,
        COALESCE(cap IS NULL OR cap <= 0, FALSE) AS c_capacity_positive,
        COALESCE(TRY_STRPTIME(open_date, '%Y-%m-%d') > TIMESTAMP '{{as_of}}', FALSE) AS c_open_date_not_in_future
FROM typed;
