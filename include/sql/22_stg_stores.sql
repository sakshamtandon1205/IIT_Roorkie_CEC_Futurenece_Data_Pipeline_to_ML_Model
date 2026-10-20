-- SQL-only staging step. Rows are never dropped here: rows that broke a Critical check
--  stay in the table with dq_status = 'QUARANTINED' and the reasons in failure_reason.

CREATE OR REPLACE TABLE stg_stores AS
SELECT store_id, store_name,
       CASE WHEN TRIM(zone) = '' THEN 'Unknown' ELSE TRIM(zone) END AS zone,
       area, pincode,
       CAST(TRY_STRPTIME(open_date, '%Y-%m-%d') AS DATE) AS open_date,
       manager_name, CAST(cap AS BIGINT) AS capacity_orders_per_day,
       CASE WHEN CONCAT_WS('; ',
        CASE WHEN c_store_id_unique THEN 'store_id_unique' END,
        CASE WHEN c_zone_is_known THEN 'zone_is_known' END) = '' THEN 'PASSED' ELSE 'QUARANTINED' END AS dq_status,
       CONCAT_WS('; ',
        CASE WHEN c_store_id_unique THEN 'store_id_unique' END,
        CASE WHEN c_zone_is_known THEN 'zone_is_known' END) AS failure_reason,
       _source_file, _run_id, _row_num, _loaded_at
FROM chk_stores;
