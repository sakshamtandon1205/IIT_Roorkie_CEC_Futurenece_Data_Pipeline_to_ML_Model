-- Reads the raw customers rows of this run and adds one TRUE/FALSE column per check (c_<check_name>).
--  Same rules as include/nammamart/checks.py::validate_customers. Nothing is changed or deleted here.

CREATE OR REPLACE TABLE chk_customers AS
WITH src AS (
    SELECT * FROM {{raw}}
    WHERE _run_id = '{{run_id}}'
)
, typed AS (
    SELECT src.*,
        UPPER(LEFT(TRIM(loyalty_tier), 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) AS tier_t,
        TRY_STRPTIME(signup_date, '%Y-%m-%d') AS signup,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _row_num) AS dup_rank
    FROM src
)
SELECT *,
        COALESCE(TRIM(city) = '', FALSE) AS c_city_not_null,
        COALESCE(dup_rank > 1, FALSE) AS c_customer_id_unique,
        COALESCE(COUNT(*) OVER (PARTITION BY LOWER(email)) > 1 AND COUNT(*) OVER (PARTITION BY customer_id) = 1, FALSE) AS c_email_unique_across_customers,
        COALESCE(tier_t NOT IN ('Bronze','Silver','Gold','Platinum'), FALSE) AS c_loyalty_tier_allowed,
        COALESCE(NOT REGEXP_FULL_MATCH(email, '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}'), FALSE) AS c_email_format_valid,
        COALESCE(NOT REGEXP_FULL_MATCH(phone, '\d{10}'), FALSE) AS c_phone_is_10_digits,
        COALESCE(NOT REGEXP_FULL_MATCH(pincode, '\d{6}'), FALSE) AS c_pincode_is_6_digits,
        COALESCE(is_active NOT IN ('Y','N'), FALSE) AS c_is_active_is_y_or_n,
        COALESCE(signup IS NULL, FALSE) AS c_signup_date_parses,
        COALESCE(tier_t IN ('Bronze','Silver','Gold','Platinum') AND loyalty_tier <> tier_t, FALSE) AS c_loyalty_tier_casing_consistent,
        COALESCE(signup > TIMESTAMP '{{as_of}}', FALSE) AS c_signup_date_not_in_future
FROM typed;
