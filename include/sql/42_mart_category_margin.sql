-- Business question 2: revenue and gross margin by product category.
-- Orders of a product that was rejected from the catalogue show up under 'Unknown' with no margin.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT COALESCE(p.category, 'Unknown') AS category,
       ROUND(SUM(f.net_revenue), 2) AS revenue,
       ROUND(SUM(f.gross_margin), 2) AS gross_margin,
       ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 2) AS margin_pct
FROM elt_fact_orders f
LEFT JOIN stg_products p ON p.product_id = f.product_id AND p.dq_status = 'PASSED'
GROUP BY 1;
