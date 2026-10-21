# NammaMart ETL and ELT Pipelines: Submission

Saksham Tandon, FDAIE Module 2

## Part 0: Discovery and data profiling

### How I profiled the data

I wrote `include/profiling/profile_data.py` and ran it before writing any pipeline code. It reads all six CSV files as plain text, so nothing is converted or fixed silently. For every file it counts rows, blanks and duplicate keys, and it prints the min and max of each numeric and date column and the distinct values of each category column. It then checks the four relationships between files, recomputes `order_amount` for every order, and tests the client's business rules. Each problem found is written to the issues log below, with the quality dimension and a severity.

The run found 41 types of issues in all five data files. The increment file (`orders_2026-10-01_increment.csv`) passed every check. It has 160 rows: 150 are new orders and 10 are corrections to older orders, each changing a status from DELIVERED to RETURNED.

| File | Issue types | Rows affected |
|---|---|---|
| customers | 8 | 53 |
| orders | 17 | 144 |
| payments | 9 | 100 |
| products | 5 | 15 |
| stores | 2 | 2 |

The row-level evidence for every count is in `docs/part0/data_issues_rows.csv`. The full console output is in `docs/part0/profile_report.txt`.

### Data Issues Log

Critical rows go to quarantine with a reason. Warning rows are loaded and logged.

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
| 40 | stores | S12 | capacity_orders_per_day | capacity_orders_per_day is zero or missing | 1 | Accuracy | load the store, skip it in the capacity check, ask the client | Warning |
| 41 | stores | S07 | zone | zone is blank (store cannot be placed in a zone) | 1 | Completeness | load with zone = 'Unknown' and ask the client (quarantining would drop the store's orders) | Warning |

### Other things I found while profiling

- Joining without fixing duplicates would count revenue twice. Two product ids (P0030 and P0140) appear twice, so 178 order rows would match two product rows. Five customer ids appear twice, so 29 order rows would match two customer rows.
- One product has a negative `mrp` (-203). Eleven orders use it.
- All 68 REFUNDED payments belong to RETURNED orders, which is consistent. The 44 FAILED payments all belong to DELIVERED orders, which is not.
- Delivery times above 120 minutes are 480, 900 and 1440. These look like values entered in the wrong unit, not real deliveries.

### Three questions for the client

1. **What does a zero or negative quantity mean?** Ten orders have a quantity of 0, -1, -2 or -3. Nine of them have a SUCCESS payment with a negative amount. Are these returns that should reduce revenue, or data-entry errors? Until I hear back, I quarantine them as Critical and keep them out of revenue.

2. **Which copy of a duplicated customer or product is the current one?** The five duplicate customers have different phone numbers or areas in each copy. The two duplicate products have one copy named "(old)" with a different `mrp`. Until I hear back, I keep the first customer row and the product row without "(old)".

3. **Does a delivered order count as revenue if its payment failed or is missing?** Forty-four delivered orders have a FAILED payment and 27 have no payment row. My reconciliation table lists every one of them either way. The answer decides whether they stay in the revenue total.

## Part C: Scheduling

Both DAGs use `CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata")` and a timezone-aware `start_date` of `pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`. I used the trigger timetable and not a plain cron string, because the timezone sits inside the schedule object and cannot be lost if someone changes the DAG's start date later.

**What is the cron expression?** `0 9 * * 1-6`, read in Asia/Kolkata. That is 9:00 AM, Monday to Saturday, which leaves 30 minutes before the 9:30 stand-up. A run takes about 10 to 20 seconds on my laptop.

**What time does the run show in UTC in the Airflow UI?** 03:30 UTC, because IST is UTC+5:30. The first scheduled run on my machine was `scheduled__2026-10-20T03:30:00+00:00`. It started on its own with nobody clicking Trigger. Screenshots 04 and 07 show the schedule for each DAG.

**What would happen at 9 AM if I had left the `start_date` in UTC?** The cron would be read as UTC, so the run would fire at 09:00 UTC, which is 14:30 IST. The numbers would arrive five hours after the stand-up, and nothing would look broken in the UI. The same mistake also shifts the day boundary: a run at 14:30 IST would belong to a different logical date than the one the business expects.

**What happens if my laptop or Docker is off at 9 AM?** Nothing runs while the scheduler is down. I checked Airflow's `CronTriggerTimetable` code to see what happens next with `catchup=False`: when the scheduler comes back it creates one run for the most recent missed 9 AM slot and skips the older ones. If I open the laptop at 11 AM, today's run starts straight away, and if it was shut for three days only the latest slot runs. That is the right behaviour for this client, but it also means a laptop is not a production scheduler. The client's 9:30 deadline needs an always-on Airflow, and I say so in the recommendation.

**Why `catchup=False`?** The stand-up needs today's number, and an old number is no use to anyone. With catchup on, a restart after a long gap would start dozens of runs back to back for dates that no longer matter. The pipelines read the same files every time, so those runs would also rewrite the same rows again. I also set `max_active_runs=1`, because DuckDB allows only one writer, and two runs at once would fight over the file.

**Retries and timeouts.** `default_args` has `retries=2`, `retry_delay=2 minutes` and `execution_timeout=10 minutes`. A short delay is enough for the faults that fix themselves, such as a file still being copied or the DuckDB file being locked for a moment. Ten minutes is far above the real run time, so a hung task is killed long before 9:30. A bad schema or a missing column is different: that can never fix itself, so the extract tasks raise `AirflowFailException`, which skips the retries and fails at once (Experiments 4 and 6).

## Part E: Data governance

**1. Ownership.** The orders and payments files belong to the order management team that produces them, and finance should own the payment and revenue definitions. Customers belong to the CRM or growth team, products to the category or catalogue team, and stores to store operations. The warehouse tables, the pipelines and `dq_results` belong to the data engineering team, which is where I sit. A change to a quality rule, such as the 120-minute delivery limit, should be proposed by data engineering and approved by the owner of the data it affects. For the delivery limit that is store operations, who know whether a 3-hour delivery can be real. The change is made through a pull request, so there is a record of who approved it and when.

**2. Data dictionary.** Types are the DuckDB types. PII is yes only where a value can identify a person.

| Table | Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|---|
| etl_fact_orders | order_id | VARCHAR | One row per order | orders.csv | unique | no |
| etl_fact_orders | order_ts, order_date | TIMESTAMP, DATE | When the order was placed; the date part | orders.csv | not in the future | no |
| etl_fact_orders | customer_id, store_id, product_id | VARCHAR | Keys to the three dimensions | orders.csv | must exist in the dimension | no (pseudonymous id) |
| etl_fact_orders | quantity | BIGINT | Units ordered | orders.csv | greater than 0 | no |
| etl_fact_orders | unit_price, discount_pct | DOUBLE | Price per unit; discount in percent | orders.csv | discount 0 to 100 | no |
| etl_fact_orders | order_amount | DOUBLE | Amount after discount | orders.csv | quantity x unit_price x (1 - discount/100) | no |
| etl_fact_orders | payment_mode | VARCHAR | How the customer paid | orders.csv, standardised | UPI, Card, Cash on Delivery, Wallet | no |
| etl_fact_orders | order_status | VARCHAR | Final status | orders.csv | DELIVERED, CANCELLED, RETURNED | no |
| etl_fact_orders | channel | VARCHAR | App or web channel | orders.csv | as received | no |
| etl_fact_orders | delivery_minutes | DOUBLE | Minutes from order to delivery | orders.csv | 1 to 120, required for DELIVERED | no |
| etl_fact_orders | net_revenue | DOUBLE | order_amount if DELIVERED, otherwise 0 | derived | 0 or more | no |
| etl_fact_orders | gross_margin | DOUBLE | net_revenue minus quantity x cost_price | derived | can be negative | no |
| etl_fact_orders | within_15_min | BOOLEAN | DELIVERED in 15 minutes or less | derived | true, false | no |
| etl_fact_orders | source_file, run_id | VARCHAR | Which file and which run wrote the row | pipeline | | no |
| etl_dim_customer | customer_id | VARCHAR | One row per customer | customers.csv | unique | no |
| etl_dim_customer | customer_name_masked | VARCHAR | Priya Hegde becomes Priya H. | customers.csv, masked | | yes (masked) |
| etl_dim_customer | email_masked, phone_masked | VARCHAR | pr***@example.com, 97******21 | customers.csv, masked | | yes (masked) |
| etl_dim_customer | city, area, pincode | VARCHAR | Delivery locality; pincode is 6 digits | customers.csv | blank city becomes Unknown | yes (location) |
| etl_dim_customer | signup_date | DATE | When the customer joined | customers.csv | not in the future | no |
| etl_dim_customer | loyalty_tier | VARCHAR | Current tier | customers.csv, title-cased | Bronze, Silver, Gold, Platinum | no |
| etl_dim_customer | is_active | VARCHAR | Whether the account is active | customers.csv | as received | no |
| etl_dim_customer_scd2 | valid_from, valid_to, is_current | DATE, DATE, BOOLEAN | Period during which a tier or locality was true | derived (bonus) | one current row per customer | no |
| etl_dim_product | product_id | VARCHAR | One row per product | products.csv | unique | no |
| etl_dim_product | product_name, brand, sub_category, unit | VARCHAR | Descriptive fields | products.csv | | no |
| etl_dim_product | category | VARCHAR | Product category, never blank | products.csv | six categories | no |
| etl_dim_product | mrp, cost_price | DOUBLE | Maximum retail price; what NammaMart pays | products.csv | mrp above 0, cost_price at most mrp | no |
| etl_dim_product | is_perishable, launch_date | BOOLEAN, DATE | | products.csv | | no |
| etl_dim_store | store_id, store_name | VARCHAR | One row per dark store | stores.csv | unique | no |
| etl_dim_store | zone | VARCHAR | North, South, East, West | stores.csv | blank becomes Unknown | no |
| etl_dim_store | area, pincode, open_date | VARCHAR, VARCHAR, DATE | Store location | stores.csv | | no |
| etl_dim_store | manager_name | VARCHAR | Store manager | stores.csv | | yes (staff name) |
| etl_dim_store | capacity_orders_per_day | BIGINT | Orders the store can handle in a day | stores.csv | 0 means unknown | no |

The ELT staging and mart tables use the same column names and meanings. The extra columns are `dq_status`, `failure_reason`, `_source_file`, `_run_id` and `_row_num`, which belong to the pipeline and hold no personal data.

**3. Lineage.** Take the `net_revenue` of one store on one day in `mart_daily_store_kpis`. In ELT the path is: the mart row is a `GROUP BY` over the view `elt_fact_orders`, which reads `stg_orders` rows with `dq_status = 'PASSED'`. Each staging row carries `_source_file`, `_run_id` and `_row_num`, and the same three columns sit on the matching `raw_orders` row, so one join lists every raw row behind the number. In ETL the fact table has `source_file` and `run_id`, so I can name the file and the run, but not the row. I can still find the row from the `order_id`. ELT is easier to trace, because every step is a table I can query and the row number survives each step. In ETL the cleaning happens in pandas in memory, so the middle steps leave no trace unless I write them out.

**4. Personal data.** In the dataset the personal columns are customer `full_name`, `email` and `phone` (direct identifiers), and `city`, `area` and `pincode`, which narrow a person down to a neighbourhood. `manager_name` in stores is a staff name. `customer_id` is only a code, but it links all of a person's orders, so I treat it as personal data too. I mask name, email and phone before they reach any table that people query (`Priya Hegde` becomes `Priya H.`, `pr***@example.com`, `97******21`). Masking keeps the shape of the value, so analysts can still count customers, check email format and spot duplicates, and nobody can phone a customer from a dashboard. Deleting the columns would lose that, and masking does not need a second lookup table to be useful. The law that applies is India's Digital Personal Data Protection Act, 2023. As I understand it, it expects NammaMart to collect data only for a stated purpose and with consent, to collect no more than needed, to keep it accurate, to protect it with reasonable security safeguards, to tell the regulator and the person if there is a breach, and to delete it when the purpose ends or the person withdraws consent. I am not a lawyer, so the client's legal team should confirm this.

One honest gap: the assignment asks for `raw_customers` to be an exact copy, so the ELT raw table in the DuckDB file holds unmasked names, emails and phone numbers. The warehouse file is not committed to git (it is in `.gitignore`), and nothing downstream reads `raw_customers` except the staging SQL, but the raw table is still the most sensitive object in my setup.

**5. Access control.** Raw tables: the data engineering team only, since raw customers holds unmasked phone numbers. Nobody else needs it, and an analyst who needs a phone number for a real business reason should ask the owner and be logged. Staging: data engineering and analysts doing quality investigations, because it holds masked values and the failure reasons. Marts: everyone who uses the reports, including finance and leadership. Quarantine files and `etl_quarantine_log`: data engineering and the owner of that dataset, who has to fix the problem at the source. In my files the quarantine copy has name, email and phone masked.

**6. Retention.** This is my proposal and the client should confirm it with legal and finance. Raw data: 13 months, which allows a year-on-year comparison plus a month of margin. Raw customers with unmasked phone numbers: 90 days, then keep only the masked version. Quarantine files: 90 days after the owner confirms the fix. Masked customer data in the marts: for as long as the customer is active, then two years. After the period, data is deleted by a scheduled job that writes to the audit table, not by hand. Data under a legal or tax hold is kept until the hold ends.

**7. Secrets.** The Slack webhook is read with `Variable.get("SLACK_WEBHOOK_URL", default=None)` in `include/nammamart/alerts.py`. The value lives in Airflow, not in the repo: set under Admin, Variables, or as the environment variable `AIRFLOW_VAR_SLACK_WEBHOOK_URL`. In this setup I did not set a real webhook, so the alert prints to the task log instead (screenshot `exp04_alert_in_log`). A webhook URL in the DAG file or on GitHub lets anyone who finds it post to the company's Slack channel, and once a secret is in git history it stays there even if the file is changed later, so the only fix is to rotate it. The same applies to any database password or API key.

**8. Audit: one rejected order, shown from receipt to revenue.** I use `ORD002255`, which has a blank `customer_id`.

In ELT, one SQL path answers everything:
```sql
SELECT * FROM raw_orders WHERE order_id = 'ORD002255';          -- exactly as received, all text
SELECT order_id, dq_status, failure_reason FROM stg_orders WHERE order_id = 'ORD002255';
SELECT COUNT(*) FROM elt_fact_orders WHERE order_id = 'ORD002255';   -- 0, it is not in the revenue
```
In ETL, the raw row is the line in `orders.csv` (kept in git), the rejected copy is in `include/quarantine/saksham/orders_failed_<run_id>.csv` with a `failure_reason` column, and `etl_quarantine_log` records the file, the row key and the reason for every run. The proof that it is not in revenue is `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id = 'ORD002255'` returning 0. ETL needs three places to answer (source file, quarantine file, log table), while ELT needs one database. The ETL quarantine copy has personal columns masked, so for the exact original I go back to the source file.

**9. Change history: DELIVERED on 3 September, RETURNED on 1 October.** After the increment load, both pipelines overwrite the status with RETURNED, so `net_revenue` for that order becomes 0 and September revenue falls by its amount. In my data ten orders are corrected this way (for example ORD000017 on 1 September and ORD000271 on 4 September). See the Experiment 3 numbers in Part F. Finance wants both views: the September figure as it was reported at the time (so the stand-up numbers from September can still be explained) and the restated figure that reflects the return. I would keep both in two ways. First, the raw layer already holds both versions, because raw is append-only and the original DELIVERED row and the correcting row are both there with their `_run_id`. Second, I would add a table of order status history, one row per status change with the date it was received (the same idea as the SCD Type 2 customer table I built as a bonus). Then a report can ask for revenue "as of 30 September" or "as restated today".
