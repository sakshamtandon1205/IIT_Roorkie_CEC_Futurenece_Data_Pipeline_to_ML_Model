-- SQL-only staging step. Rows are never dropped here: rows that broke a Critical check
--  stay in the table with dq_status = 'QUARANTINED' and the reasons in failure_reason.

CREATE OR REPLACE TABLE stg_products AS
SELECT product_id, product_name, category, sub_category, brand, mrp_n AS mrp, cost_n AS cost_price, unit,
       is_perishable = 'True' AS is_perishable,
       CAST(TRY_STRPTIME(launch_date, '%Y-%m-%d') AS DATE) AS launch_date,
       CASE WHEN CONCAT_WS('; ',
        CASE WHEN c_category_not_null THEN 'category_not_null' END,
        CASE WHEN c_product_id_unique THEN 'product_id_unique' END,
        CASE WHEN c_mrp_positive THEN 'mrp_positive' END,
        CASE WHEN c_cost_price_positive THEN 'cost_price_positive' END,
        CASE WHEN c_cost_price_not_above_mrp THEN 'cost_price_not_above_mrp' END) = '' THEN 'PASSED' ELSE 'QUARANTINED' END AS dq_status,
       CONCAT_WS('; ',
        CASE WHEN c_category_not_null THEN 'category_not_null' END,
        CASE WHEN c_product_id_unique THEN 'product_id_unique' END,
        CASE WHEN c_mrp_positive THEN 'mrp_positive' END,
        CASE WHEN c_cost_price_positive THEN 'cost_price_positive' END,
        CASE WHEN c_cost_price_not_above_mrp THEN 'cost_price_not_above_mrp' END) AS failure_reason,
       _source_file, _run_id, _row_num, _loaded_at
FROM chk_products;
