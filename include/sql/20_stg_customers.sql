-- SQL-only staging step. Rows are never dropped here: rows that broke a Critical check
--  stay in the table with dq_status = 'QUARANTINED' and the reasons in failure_reason.

CREATE OR REPLACE TABLE stg_customers AS
SELECT customer_id,
       CASE WHEN TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g')) = '' THEN ''
         WHEN POSITION(' ' IN TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g'))) = 0 THEN TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g'))
         ELSE SPLIT_PART(TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g')), ' ', 1) || ' ' ||
              LEFT(LIST_EXTRACT(STRING_SPLIT(TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g')), ' '), -1), 1) || '.' END AS customer_name_masked,
       CASE WHEN TRIM(email) = '' THEN ''
         WHEN POSITION('@' IN TRIM(email)) = 0 THEN LEFT(TRIM(email), 1) || REPEAT('*', GREATEST(LENGTH(TRIM(email)) - 1, 0))
         ELSE LEFT(SPLIT_PART(TRIM(email), '@', 1), 2) || '***@' || SUBSTR(TRIM(email), POSITION('@' IN TRIM(email)) + 1) END AS email_masked,
       CASE WHEN TRIM(phone) = '' THEN ''
         WHEN LENGTH(TRIM(phone)) <= 4 THEN REPEAT('*', LENGTH(TRIM(phone)))
         ELSE LEFT(TRIM(phone), 2) || REPEAT('*', LENGTH(TRIM(phone)) - 4) || RIGHT(TRIM(phone), 2) END AS phone_masked,
       CASE WHEN TRIM(city) = '' THEN 'Unknown' ELSE TRIM(city) END AS city,
       area, pincode,
       CAST(signup AS DATE) AS signup_date,
       tier_t AS loyalty_tier,
       is_active,
       CASE WHEN CONCAT_WS('; ',
        CASE WHEN c_customer_id_unique THEN 'customer_id_unique' END,
        CASE WHEN c_loyalty_tier_allowed THEN 'loyalty_tier_allowed' END,
        CASE WHEN c_signup_date_parses THEN 'signup_date_parses' END,
        CASE WHEN c_signup_date_not_in_future THEN 'signup_date_not_in_future' END) = '' THEN 'PASSED' ELSE 'QUARANTINED' END AS dq_status,
       CONCAT_WS('; ',
        CASE WHEN c_customer_id_unique THEN 'customer_id_unique' END,
        CASE WHEN c_loyalty_tier_allowed THEN 'loyalty_tier_allowed' END,
        CASE WHEN c_signup_date_parses THEN 'signup_date_parses' END,
        CASE WHEN c_signup_date_not_in_future THEN 'signup_date_not_in_future' END) AS failure_reason,
       _source_file, _run_id, _row_num, _loaded_at
FROM chk_customers;
