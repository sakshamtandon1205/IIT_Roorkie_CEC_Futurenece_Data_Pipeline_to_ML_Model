-- When a product_id is repeated, the row whose name does NOT end with '(old)' is kept (dup_rank = 1).

CREATE OR REPLACE TABLE chk_products AS
WITH src AS (
    SELECT * FROM {{raw}}
    WHERE _run_id = '{{run_id}}'
)
, typed AS (
    SELECT src.*,
        TRY_CAST(mrp AS DOUBLE) AS mrp_n, TRY_CAST(cost_price AS DOUBLE) AS cost_n,
        product_name LIKE '%(old)%' AS is_old,
        ROW_NUMBER() OVER (PARTITION BY product_id
                           ORDER BY (product_name LIKE '%(old)%'), _row_num) AS dup_rank
    FROM src
)
SELECT *,
        COALESCE(TRIM(category) = '', FALSE) AS c_category_not_null,
        COALESCE(dup_rank > 1, FALSE) AS c_product_id_unique,
        COALESCE(TRIM(category) <> '' AND category NOT IN ('Fruits & Vegetables','Dairy & Bakery','Staples','Snacks & Beverages','Household','Personal Care'), FALSE) AS c_category_is_known,
        COALESCE(is_perishable NOT IN ('True','False'), FALSE) AS c_is_perishable_is_true_or_false,
        COALESCE(mrp_n IS NULL OR mrp_n <= 0, FALSE) AS c_mrp_positive,
        COALESCE(cost_n IS NULL OR cost_n <= 0, FALSE) AS c_cost_price_positive,
        COALESCE(cost_n > mrp_n, FALSE) AS c_cost_price_not_above_mrp,
        COALESCE(is_old, FALSE) AS c_no_old_catalogue_rows,
        COALESCE(TRY_STRPTIME(launch_date, '%Y-%m-%d') > TIMESTAMP '{{as_of}}', FALSE) AS c_launch_date_not_in_future
FROM typed;
