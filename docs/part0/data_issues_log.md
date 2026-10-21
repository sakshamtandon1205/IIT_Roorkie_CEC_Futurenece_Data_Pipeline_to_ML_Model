| # | File | Row key (examples) | Column | Problem | Rows | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|---|---|
| 1 | customers | C00400, C00398, C00038 | signup_date | signup_date is in the future | 3 | Timeliness | signup_date <= extract date | Critical |
| 2 | customers | C00103, C00103, C00252 | customer_id | customer_id appears more than once (all copies shown) | 10 | Uniqueness | copies disagree on phone/area: keep one row per customer (ask client which is current) | Critical |
| 3 | customers | C00355, C00394, C00233 | city | city is blank | 6 | Completeness | load; fill 'Unknown' in the dimension | Warning |
| 4 | customers | C00199, C00171, C00133 | loyalty_tier | loyalty_tier has inconsistent casing (BRONZE, bronze, SILVER) | 8 | Consistency | title-case everything | Warning |
| 5 | customers | C00337, C00045, C00327 | email | same email on two different customer_ids | 4 | Uniqueness | investigate possible duplicate customer | Warning |
| 6 | customers | C00337, C00307, C00063 | email | email format is invalid | 12 | Validity | regex check; mask anyway | Warning |
| 7 | customers | C00151, C00240, C00095 | phone | phone is not exactly 10 digits | 7 | Validity | 10 digits only | Warning |
| 8 | customers | C00132, C00083, C00166 | pincode | pincode is not exactly 6 digits | 3 | Validity | 6 digits only | Warning |
| 9 | orders | ORD002103, ORD002305, ORD002004 | order_amount | order_amount differs from quantity x unit_price x (1 - discount/100) | 18 | Accuracy | recompute and compare, tolerance 0.01 -> quarantine | Critical |
| 10 | orders | ORD000469, ORD001296, ORD001459 | quantity | quantity is zero or negative | 10 | Accuracy | quantity > 0 -> quarantine (ask client if negative = return) | Critical |
| 11 | orders | ORD001037, ORD002101, ORD000455 | delivery_minutes | delivery_minutes above 120 (impossible, hours not minutes) | 6 | Accuracy | delivery_minutes <= 120 -> quarantine | Critical |
| 12 | orders | ORD002255, ORD000701, ORD000997 | customer_id | customer_id is blank | 18 | Completeness | customer_id NOT NULL -> quarantine | Critical |
| 13 | orders | ORD001554, ORD001800, ORD000731 | delivery_minutes | DELIVERED order has no delivery_minutes | 8 | Completeness | DELIVERED requires delivery_minutes -> quarantine | Critical |
| 14 | orders | ORD000362 | order_ts | order_ts is blank | 1 | Completeness | order_ts NOT NULL -> quarantine | Critical |
| 15 | orders | ORD001113, ORD000737, ORD001398 | order_id | DELIVERED order has no payment row | 27 | Consistency | list in reconciliation table (revenue not confirmed by gateway) | Critical |
| 16 | orders | ORD000986, ORD001058, ORD001685 | product_id | product_id not found in products | 9 | Consistency | FK check against products -> quarantine | Critical |
| 17 | orders | ORD001799, ORD001046, ORD001492 | store_id | store_id not found in stores | 5 | Consistency | FK check against stores -> quarantine | Critical |
| 18 | orders | ORD002119, ORD001693, ORD001089 | order_ts | order_ts is in the future | 7 | Timeliness | order_ts <= 2026-10-01 -> quarantine | Critical |
| 19 | orders | ORD001462, ORD001441, ORD001692 | order_id | order_id appears more than once (repeat of an earlier row) | 12 | Uniqueness | keep the first row, quarantine repeats (they would double revenue) | Critical |
| 20 | orders | ORD000403, ORD001610, ORD000534 | order_ts | order_ts is not a valid timestamp | 5 | Validity | order_ts must parse as YYYY-MM-DD HH:MM:SS -> quarantine | Critical |
| 21 | orders | ORD000500, ORD001970, ORD001637 | order_status | order_status not in DELIVERED/CANCELLED/RETURNED | 3 | Validity | reject unknown statuses -> quarantine | Critical |
| 22 | orders | ORD002103, ORD001468, ORD001237 | discount_pct | discount_pct outside 0-100 | 3 | Validity | 0 <= discount_pct <= 100 -> quarantine | Critical |
| 23 | orders | ORD000159, ORD001364, ORD000433 | payment_mode | payment_mode has wrong casing/spaces | 7 | Validity | map to the standard value (upi -> UPI) | Warning |
| 24 | orders | ORD001812, ORD001414, ORD001640 | payment_mode | payment_mode is a typo or short form (Cashh, cod) | 3 | Validity | lookup map: cod/Cashh -> Cash on Delivery (confirm with client) | Warning |
| 25 | orders | ORD001066, ORD000153 | order_status | order_status has wrong casing/spaces | 2 | Validity | TRIM + UPPER, then accept | Warning |
| 26 | payments | PAY0001568, PAY0002175, PAY0000116 | amount_paid | amount_paid is missing, non-numeric or not positive | 10 | Accuracy | amount_paid > 0 | Critical |
| 27 | payments | PAY0002092, PAY0000177, PAY0000259 | payment_status | payment FAILED but the order is DELIVERED (revenue not collected) | 44 | Consistency | list in reconciliation table; finance decides if it counts as revenue | Critical |
| 28 | payments | PAY0001568, PAY0000354, PAY0000779 | amount_paid | SUCCESS payment amount does not equal the order's order_amount | 14 | Consistency | amount_paid = order_amount for SUCCESS -> reconciliation table | Critical |
| 29 | payments | PAY0002219, PAY0002220, PAY0002223 | order_id | payment refers to an order_id that is not in orders | 8 | Consistency | FK check against orders -> quarantine | Critical |
| 30 | payments | PAY0001870, PAY0001966, PAY0000688 | order_id | SUCCESS payment belongs to an order with an invalid status (Pending??) | 3 | Consistency | reconciliation table; resolve the order status first | Critical |
| 31 | payments | PAY0000396, PAY0000127, PAY0001908 | payment_ts | payment_ts is in the future | 7 | Timeliness | payment_ts <= extract date | Critical |
| 32 | payments | PAY0000509, PAY0001651, PAY0002000 | payment_id | payment_id appears more than once | 4 | Uniqueness | keep first, quarantine repeats | Critical |
| 33 | payments | PAY0002188, PAY0001850, PAY0000817 | payment_mode | payment_mode has wrong casing/spaces | 7 | Validity | map to the standard value | Warning |
| 34 | payments | PAY0001954, PAY0000522, PAY0000044 | payment_mode | payment_mode is a typo or short form (Cashh, cod) | 3 | Validity | lookup map: cod/Cashh -> Cash on Delivery (confirm with client) | Warning |
| 35 | products | P0019, P0073, P0074 | cost_price | cost_price is higher than mrp | 5 | Accuracy | cost_price <= mrp | Critical |
| 36 | products | P0109 | mrp | mrp missing or not positive | 1 | Accuracy | mrp > 0 | Critical |
| 37 | products | P0051, P0060, P0068 | category | category is blank | 3 | Completeness | category NOT NULL (needed for margin by category) | Critical |
| 38 | products | P0030, P0140, P0030 | product_id | product_id appears more than once (all copies shown) | 4 | Uniqueness | keep the row whose name does not end with '(old)'; one row per product | Critical |
| 39 | products | P0030, P0140 | product_name | old catalogue row left in the file (name ends with '(old)', different mrp) | 2 | Consistency | drop '(old)' rows from dim_product, keep for audit in raw | Warning |
| 40 | stores | S12 | capacity_orders_per_day | capacity_orders_per_day is zero or missing | 1 | Accuracy | capacity > 0 (needed for the capacity bonus check) | Critical |
| 41 | stores | S07 | zone | zone is blank (store cannot be placed in a zone) | 1 | Completeness | zone NOT NULL (daily revenue by zone needs it) | Critical |
