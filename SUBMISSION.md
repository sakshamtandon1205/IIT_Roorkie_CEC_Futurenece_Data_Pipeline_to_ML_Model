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

**What is the cron expression?** `0 9 * * 1-6`, read in Asia/Kolkata. That is 9:00 AM, Monday to Saturday, which leaves 30 minutes before the 9:30 stand-up. An ETL run takes about 10 to 17 seconds on my laptop and an ELT run 30 to 50 seconds.

**What time does the run show in UTC in the Airflow UI?** 03:30 UTC, because IST is UTC+5:30. The first scheduled run on my machine was `scheduled__2026-10-20T03:30:00+00:00`. It started on its own with nobody clicking Trigger. Screenshots 04 and 07 show the schedule for each DAG.

**What would happen at 9 AM if I had left the `start_date` in UTC?** The cron would be read as UTC, so the run would fire at 09:00 UTC, which is 14:30 IST. The numbers would arrive five hours after the stand-up, and nothing would look broken in the UI. The same mistake also shifts the day boundary: a run at 14:30 IST would belong to a different logical date than the one the business expects.

**What happens if my laptop or Docker is off at 9 AM?** Nothing runs while the scheduler is down. I checked Airflow's `CronTriggerTimetable` code to see what happens next with `catchup=False`: when the scheduler comes back it creates one run for the most recent missed 9 AM slot and skips the older ones. If I open the laptop at 11 AM, today's run starts straight away, and if it was shut for three days only the latest slot runs. I saw this happen. `pipeline_runs` has one scheduled ELT run for the 20 October slot (`scheduled__2026-10-20T03:30:00`), and it started at 02:47 on 21 October, because my Docker was not running at 9 AM on the 20th. It is the only run for any day before the 21st, so Airflow did not create runs for the missed days since the 1 September start date. The 21 October slot ran by itself at 09:02 IST, about two minutes after the cron time, when Airflow was up. That is the right behaviour for this client, but it also means a laptop is not a production scheduler. The client's 9:30 deadline needs an always-on Airflow, and I say so in the recommendation.

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

## Part F: Observability tests

### Run audit

The last task of each DAG is `audit`, with `trigger_rule="all_done"`, so it runs even when something upstream failed. It writes one row per run to `pipeline_runs` and then fails on purpose if any task failed. I added that second step after my first version showed a green run for a run that had a failed task, because `audit` was the last task and it succeeded. The numbers below come from `pipeline_runs` and `include/tools/history.py`. "Loaded" in ETL counts rows upserted into the fact table, not new rows.

| Run | ETL extracted / loaded / quarantined | ETL status | ELT extracted / loaded / quarantined | ELT status |
|---|---|---|---|---|
| 1 and 2 Clean run (and re-run) | 5,212 / 2,310 / 162 | success | 5,212 / 5,212 / 162 | success |
| 3 Increment loaded | 5,372 / 2,470 / 162 | success | 5,372 / 5,372 / 162 | success |
| 5 Five bad rows added | 5,217 / 2,300 / 167 | success | 5,217 / 5,217 / 167 | success |
| 6 Column renamed | 2,800 / 0 / 0 | failed | 2,800 / 2,800 / 0 | failed |
| 7 Only 50 orders | 2,850 / 48 / 2,201 | success | 2,850 / 2,850 / 2,201 | success |
| 8 S03 prices x100 | 5,212 / 2,153 / 310 | success | 5,212 / 5,212 / 310 | success |
| 9 Run date set to 1 Nov | 5,212 / 2,302 / 158 | success | 5,212 / 5,212 / 158 | success |
| Final clean run | 5,212 / 2,300 / 162 | success | 5,212 / 5,212 / 162 | success |

After the clean runs, ETL loads 2,300 and not 2,310 because the ten corrected orders from the increment file are protected from being overwritten by the older `orders.csv` (see Experiment 3). Experiment 4 is not in the table, and I explain why in Experiment 10. Every run writes 63 rows to `dq_results` per pipeline (84 when the increment file is loaded, since it adds 21 checks). In every clean run, 24 checks are in FAIL and 14 in WARN, and ETL and ELT give identical results for every check.

### Experiment 1: Happy path

**Expected.** Both DAGs finish green. The five extract tasks (six with the increment file) run in parallel. Each file shows rows in, passed and quarantined.

**Observed.** Both finished green. In the Gantt view the extract tasks overlap and `validate` starts only when the last one ends. ETL took 9 to 17 seconds, apart from the very first run, which took 134 seconds. ELT took 32 to 50 seconds, because it has more tasks: six raw loads, five staging tables, a merge, the checks, six marts and a comparison. ETL per file: orders 2,412 in, 2,310 passed, 102 quarantined; payments 2,228 in, 2,186 passed, 42 quarantined; customers 405 in, 397 passed, 8 quarantined; products 152 in, 142 passed, 10 quarantined; stores 15 in, 15 passed. That is 162 quarantined in all, and ELT rejected the same 162 in staging. ELT loaded all 5,212 rows to raw.

**Where I saw it.** Graph and Gantt views (`exp01_etl_gantt`, `exp01_elt_gantt`), the `validate` log (`exp01_etl_validate_log`), `pipeline_runs` and `dq_results`.

**What I would change.** Nothing for this run. The first run was slow, but later runs were not.

### Experiment 2: Re-run (idempotency)

**Expected.** ETL fact rows stay the same. ELT raw tables grow, because raw is append-only.

**Observed.** ETL's fact table stayed at 2,310 rows and the revenue total did not move. `rows_loaded` stayed at 2,310 too, which shows it counts upserts and not new rows. In ELT, every run appended another 2,412 rows to `raw_orders` (the per-run list at the end of `show.py exp` has one entry per run). Staging and the fact view did not change, because they read only the latest run's raw rows. That is correct: raw is the audit copy and the client asked for it to be kept untouched. It does cost storage.

**Where I saw it.** `show.py exp` (`exp02_rerun_counts`).

**What I would change.** Report inserted and updated rows separately in `pipeline_runs`, so a re-run visibly shows zero new rows. For raw, load only new data and delete old runs on the retention schedule in Part E.

### Experiment 3: Incremental load

**Expected.** 150 orders inserted and 10 updated, with no duplicates.

**Observed.** ETL extracted 5,372 rows (5,212 plus 160) and upserted 2,470. The fact table went from 2,310 to 2,460 rows, so 150 were new and 10 were updates. The ten corrected orders changed from DELIVERED to RETURNED, and the check for duplicates showed 2,460 rows and 2,460 distinct order ids. ELT did the same through `MERGE INTO` on `order_id` and also ended with 2,460 rows. I found a bug while testing this: a later normal run, without the increment, put the old DELIVERED rows back. I fixed it so that rows that came from the increment file are not overwritten by `orders.csv`. That is the reason ETL loads 2,300 and not 2,310 afterwards.

**Where I saw it.** `exp03_etl_load_log`, `exp03_elt_merge_log` and `show.py inc` (`exp03_increment_result`).

**What I would change.** Better inserted and updated counters, as in Experiment 2.

### Experiment 4: Missing source file

**Expected.** The extract task fails and retries, tasks after it do not run, and no wrong data is loaded.

**Observed.** I renamed `products.csv`. In ETL `extract_products` and in ELT `load_raw_products` went to `up_for_retry`, waited two minutes, retried twice, and then failed, so the failure took about four minutes. The other extract and load tasks finished green at the same time. Everything after the failed task was `upstream_failed`. `audit` still ran and wrote a failed row. The failure message was printed in the task log, because no Slack webhook is set (`exp04_alert_in_log`). In my first version the run itself still showed green, so nothing in the run list told me anything was wrong; I fixed this by making `audit` fail the run.

**Where I saw it.** Graph view during the retries and at the end (`exp04_etl_graph_retry`, `exp04_elt_graph_retry`, `exp04_etl_final_states`, `exp04_elt_final_states`), the task logs (`exp04_etl_extract_products_log`, `exp04_elt_load_raw_products_log`) and `pipeline_runs` (`exp04_pipeline_runs_failed`).

**What I would change.** I send the alert only after all retries have failed. A missing file will not fix itself in two minutes, so I would alert on the first failure and use a file sensor that waits for the file until 9:20.

### Experiment 5: New bad data

**Expected.** The five bad rows (null customer, negative quantity, unknown store, status SHIPPED, a 2027 timestamp) land in quarantine with the right reason, and `dq_results` counts them.

**Observed.** Both pipelines rejected all five, each with its own reason (`exp05_quarantine_rows`, `exp05_rejected_both`). Quarantined rows went from 162 to 167 in both. In `dq_results` five counters each went up by one: `customer_id_not_null` 18 to 19, `quantity_positive` 10 to 11, `store_exists_in_stores` 5 to 6, `order_status_allowed` 3 to 4 and `order_ts_not_in_future` 7 to 8 (`exp05_dq_results_changed`). The fact table did not change.

**Where I saw it.** The quarantine file, `etl_quarantine_log`, the `elt_rejected_rows` view and `dq_results`.

**What I would change.** Nothing. Every bad row had a reason and a check that counted it.

### Experiment 6: Schema change

**Expected.** The pipeline fails loudly and does not load wrong data.

**Observed.** I renamed `order_amount` to `total`. ETL `extract_orders` failed in 2 seconds and the whole run failed in 17 seconds. ELT `load_raw_orders` failed in 9 seconds and the run failed in 20 seconds. The error was `PermanentSchemaError: orders.csv is missing required column(s): ['order_amount']`. There was only one try, with no retries, because the extract raises `AirflowFailException` for a schema error that cannot fix itself. The run status was Failed. In ETL nothing reached the warehouse (0 rows loaded). In ELT the other four raw tables loaded (2,800 rows), but orders did not, and no staging or mart task ran.

**Where I saw it.** Graph view and the task log for each pipeline (`exp06_etl_graph_failed`, `exp06_etl_error_log`, `exp06_elt_graph_failed`, `exp06_elt_error_log`), and the `failed` rows in `pipeline_runs`.

**What I would change.** The check only looks for missing required columns. A new column, or a column that changed type, would pass silently. I would also compare column types with the last run.

### Experiment 7: Volume anomaly

**Expected.** Nothing warns when volume drops by more than 90%, so I add a check.

**Observed.** I cut orders to 50 rows. My `orders.volume_vs_last_run` check (Warning, fires below 10% of the last run) fired in both pipelines (`exp07_etl_volume_warning`, `exp07_elt_volume_warning`). Both runs still finished green, because it is only a Warning. ETL's fact table stayed at 2,460 rows and 1,211,087.30 revenue, so it showed old data. ELT rebuilt its marts from the 50 rows: 208 fact rows and 103,133.80 revenue, a fall of 91%, and S03's revenue dropped to 3,992.75 (`exp07_exp_output`). There was a side effect: 2,181 of 2,228 payments no longer had an order, so they failed `payments.order_exists_in_orders` and were quarantined as Critical.

**Where I saw it.** The `validate` log in ETL, `dq_results` for ELT, and `show.py exp`.

**What I would change.** A volume drop this large should be Critical and stop the DAG before the load or the mart rebuild. Today the warning is easy to miss and ELT publishes a revenue number that is 91% too low.

### Experiment 8: Distribution drift

**Expected.** Some check catches the change, and a mart metric moves.

**Observed.** I multiplied `unit_price` by 100 for store S03 (159 orders). Two checks fired in both pipelines: `order_amount_matches_formula` (Critical) went from 18 to 175 rows failing, and `unit_price_matches_catalogue_mrp` (Warning) from 0 to 155. The formula check caught it only because `order_amount` was left alone. If a source system had changed both columns together, only the Warning would have fired and the rows would have loaded. In ETL the bad S03 rows were quarantined and the older values stayed in the fact table (2,460 rows, 1,211,087.30), so ETL looked healthy but was stale. In ELT the rows dropped out of the marts: 2,313 fact rows and revenue of 1,134,365.25, which is 76,722.05 lower. S03's own revenue went from 80,417.80 to 3,695.75. The comparison task printed DIFFERENT on all five totals and `all_match: False`, but it does not fail the run (`exp08_elt_compare_log`, `exp08_etl_validate_log`, `exp08_exp_output`).

**Where I saw it.** `dq_results`, the `[Compare]` log in `compare_with_etl` and `show.py exp`.

**What I would change.** I have no check on the shape of the data over time. I would add a daily check per store: if a store's revenue or average price moves by more than a set percentage from its own recent average, raise a Warning. I would also make a price more than 10 times the catalogue MRP Critical.

### Experiment 9: Freshness

**Expected.** The timeliness check warns.

**Observed.** I did not edit the file. The `as_of` parameter tells the run what today's date is, so I ran both DAGs with `as_of` = 2026-11-01. The latest order is then 31 days old, which is the same condition as a stale file. `orders.latest_order_within_1_day` went to WARN in both pipelines (`exp09_etl_validate_log`, `exp09_dq_terminal`), the run configuration with the date is in `exp09_run_config`, and both runs finished green. There was a side effect I did not plan for. With the date moved forward, two orders dated 21 October (ORD001693 and ORD001089, 1,179.80 together) were no longer "in the future", so ETL loaded them. When I ran again with the normal date they stayed in the ETL fact table (2,462 rows against 2,460 in ELT), because ETL only upserts and never removes. ELT showed the right numbers again after its next run, because it rebuilds from the latest raw load. The ETL payment reconciliation also differed from ELT for the same two orders (94 against 96 payments without an order). I found this by comparing the two pipelines after the experiments, and fixed it by deleting those two rows and re-running both DAGs. The totals match again (see below).

**Where I saw it.** The `validate` log, `show.py dq`, the run's Details tab, and `show.py exp`.

**What I would change.** ETL needs a way to rebuild the fact table from the source files (a `full_refresh` option) and a rule to remove rows that no longer pass. I would not prune automatically, because after Experiment 7 that would delete 2,400 good rows.

### Experiment 10: Recovery

**Expected.** After I fix the cause and use Clear on the failed task, only the failed task and the tasks after it run again.

**Observed.** I put `products.csv` back and cleared the failed extract task in each DAG, with downstream tasks included (`exp10_etl_clear_dialog`, `exp10_elt_clear_dialog`). The tasks that had already succeeded were not run again, and the cleared tasks show a higher try number than the others (`exp10_etl_try_numbers`, `exp10_elt_try_numbers`). Both runs then finished green. One weakness showed up: the audit row is written with the run id, so the green recovery replaced the failed row in `pipeline_runs`. That is why Experiment 4 is not in the table above. I took a screenshot of the failed row before recovering (`exp04_pipeline_runs_failed`).

**Where I saw it.** Graph view, the Clear dialog and the Try Number column.

**What I would change.** Write one audit row for each attempt and not one per run, so a failed attempt is never replaced.

### The five pillars of data observability

| Pillar | What I have | Seen in | How well covered |
|---|---|---|---|
| Freshness | `latest_order_within_1_day` check, plus the run date and time in `pipeline_runs` | Experiment 9 | Fair. It only looks at orders. Nothing checks payments or customers. |
| Volume | `volume_vs_last_run`, row counts in every audit row | Experiment 7 | Weak. It detects the drop, but only as a Warning, so the run carries on. |
| Schema | Required-column check at extract time, fails fast | Experiment 6 | Strongest. It fails in seconds, with the exact column, and no retries. |
| Distribution | Formula and catalogue price checks only | Experiment 8 | Weakest. Nothing compares a store or a day with its own history. |
| Lineage | `_source_file`, `_run_id`, `_row_num` in ELT; `source_file` and `run_id` in ETL | Part E, question 3 | Good in ELT, partial in ETL (no row number). |

The best covered pillar is schema, because a bad file stops the run in under 20 seconds and says why. The weakest is distribution. Experiment 8 showed that one store's revenue could fall by 95% and the run would stay green.

## Do the ELT totals match the ETL totals?

Yes, after the final clean runs. The comparison task prints MATCH for all five metrics: 2,460 fact rows, net revenue 1,211,087.30, gross margin 279,931.92, 2,228 delivered orders and 339 reconciliation mismatches (`final_elt_vs_etl_match`). The totals matched before the experiments and they differed afterwards only because of Experiment 9: two orders loaded into ETL under the wrong run date stayed there (see above). This is also the main structural difference between the two designs. ETL keeps whatever it has loaded until something removes it, and ELT recalculates from the latest raw load every time.

## Business questions

All six questions are answered with SQL and results from both pipelines in `docs/business_questions.md`. It is produced by `include/tools/answers.py`, which also compares the full ETL and ELT results for each question and prints whether they are identical.

## The client's ten acceptance criteria

| # | Criterion | Where it is shown |
|---|---|---|
| 1 | Ready before 9:30, Monday to Saturday | Part C, screenshots 04 and 07 |
| 2 | No bad order in the finance tables | ETL fact table holds only passed rows; Experiments 5 and 8 |
| 3 | Raw records kept untouched | ELT `raw_*` tables, Experiment 2 |
| 4 | Bad rows set aside with a reason | Quarantine files, `etl_quarantine_log`, `elt_rejected_rows` (screenshot 10) |
| 5 | Re-running never double-counts | Experiment 2 |
| 6 | Increment loaded, status changes applied | Experiment 3 |
| 7 | Orders reconcile with payments | `etl_payment_reconciliation`, `mart_payment_reconciliation` (screenshot 13) |
| 8 | Personal data protected | Masking in Part E, question 4 |
| 9 | Anyone can see whether the run worked | `pipeline_runs`, task logs (screenshot 12) |
| 10 | Written recommendation | Part G |

## Bonus items

- Failure alert: `include/nammamart/alerts.py` posts to Slack with the webhook read from the Airflow Variable `SLACK_WEBHOOK_URL`. With no webhook set it prints the alert in the task log instead (`exp04_alert_in_log`).
- SCD Type 2 customers: `etl_dim_customer_scd2` keeps `valid_from`, `valid_to` and `is_current` for loyalty tier, city, area and pincode.
- Store capacity check: `orders.store_daily_orders_within_capacity` compares each store's daily orders with `capacity_orders_per_day`. No store went over capacity in this data, so the check passes.
- Data quality trend chart: `python include/tools/dq_trend.py` draws `docs/dq_trend.html` from `dq_results` and `pipeline_runs` (`dq_trend.png`).

## Part G: Recommendation to Sameer (CTO)

**Recommendation: build NammaMart on ELT, with a few ETL-style gates in front of it.** Keep every raw file as it arrives, do the cleaning and the business logic in SQL inside the warehouse, and stop the run before the marts are rebuilt when the input looks wrong.

I built both pipelines on the same six files. They gave the same answers on the same data, and the same 63 checks agreed on every run. What I learned is in how they behave when something goes wrong.

**Auditability.** This is where ELT is clearly better. When finance asks about one rejected order, ELT answers with three queries in one database: the raw row exactly as received, the rule it broke and proof it is not in the revenue. ETL needs the source file, the quarantine file and a log table, and its quarantine copy is masked, so the exact original lives somewhere else. A status change such as DELIVERED to RETURNED also keeps both versions in ELT raw. ETL overwrites the old one.

**Data quality guarantees.** Both pipelines kept bad rows out of the finance numbers. They fail differently. In Experiments 7 and 8, ETL went on showing the old numbers (2,460 rows, 1,211,087.30), which looked healthy and was stale. ELT showed the damage straight away: revenue fell to 103,133.80 when I cut the file to 50 orders. A number that is visibly wrong gets noticed at the stand-up. A stale number does not, and Experiment 9 showed ETL keeping rows it should have dropped.

**Ease of change.** In ELT a rule is a SQL file, and I can re-run it over the raw rows that are already stored, without going back to the source. When the 120-minute limit changes, the history can be recalculated. In ETL a rule change only applies to files loaded afterwards unless I run the old files again. ELT was also easier to trace (Part E, question 3).

**Performance at 1000 times the data.** I measured only the small data: 10 to 17 seconds for ETL and 30 to 50 for ELT, so I cannot give measured figures for 1000 times. ETL holds each file in memory on the Airflow worker, so it hits the memory limit first. ELT hands the work to the warehouse engine, which is built for that. ELT's own problem is that it copies the whole file into raw on every run (Experiment 2). At 1000 times, that means millions of rows copied each day, so raw has to load only new data and be partitioned by date.

**Cost.** ELT pays for more storage, because it keeps raw, and for more compute, because it rebuilds the marts on each run. ETL is cheaper to store but costs engineer time every time a number has to be explained. At this size both are trivial. At 1000 times the raw copy and the full rebuild become the main bills, which is why I suggest loading only new data and rebuilding only the days that changed.

### Top three risks of this design

1. **Personal data in raw.** ELT raw holds customer names, emails and phone numbers exactly as received. That is the most sensitive object in the warehouse. Mitigation: a separate schema that only data engineering can read, encryption, a 90-day retention, and masking or tokenising the sensitive columns as they are loaded where the business does not need the original. This also supports the client's DPDP Act obligations.
2. **A bad file quietly shrinks the marts.** In Experiment 7 the run stayed green and the revenue mart lost 91%. Mitigation: make a volume drop above 50% Critical so the DAG stops before the marts are rebuilt, add the per-store daily checks from Experiment 8, and fail the run when total revenue moves by more than a set percentage from the previous day.
3. **The platform is a single laptop-sized setup.** DuckDB takes one writer at a time, and Airflow on a laptop skips the 9 AM run when the machine is off (it happened on 20 October). Mitigation: an always-on Airflow, a warehouse that accepts concurrent writers, and an alert at the first failure and not after the retries.

If the client chooses ETL instead, the changes I would insist on are a full-refresh mode, a rule for removing rows that no longer pass, and a raw archive of every file.
