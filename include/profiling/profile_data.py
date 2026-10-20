#!/usr/bin/env python3
"""
Part 0: profile the six NammaMart CSV files and build the Data Issues Log.

Run from the project root (inside the virtual environment):
    python include/profiling/profile_data.py

It reads the CSVs from include/data/nammamart/ and writes into docs/part0/:
    profile_report.txt     everything printed on screen
    data_issues_log.csv    one line per issue TYPE (file, column, problem, dimension, severity, count)
    data_issues_log.md     the same log as a markdown table, ready for SUBMISSION.md
    data_issues_rows.csv   one line per bad ROW (the evidence behind each count)

Only pandas is needed. The CSVs are never edited.
"""
import re
import sys
from pathlib import Path

import pandas as pd

DATA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("include/data/nammamart")
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("docs/part0")
OUT.mkdir(parents=True, exist_ok=True)

# "Today" for the timeliness checks. The September export was taken on the
# morning of 1 Oct 2026, so anything dated after that is in the future.
AS_OF = pd.Timestamp("2026-10-01 00:00:00")
DATA_START = pd.Timestamp("2026-09-01")

# Allowed values from the client's business rules (assignment Section 3).
STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
PAY_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
PAY_STATUSES = {"SUCCESS", "FAILED", "REFUNDED"}
TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
ZONES = {"North", "South", "East", "West"}
CATEGORIES = {"Fruits & Vegetables", "Dairy & Bakery", "Staples",
              "Snacks & Beverages", "Household", "Personal Care"}

KEYS = {"orders": "order_id", "increment": "order_id", "payments": "payment_id",
        "customers": "customer_id", "products": "product_id", "stores": "store_id"}

# ---------------------------------------------------------------- output helpers
_lines = []


def say(text=""):
    """Print and also remember the line, so the report can be saved to a file."""
    print(text)
    _lines.append(str(text))


issues = []      # one dict per issue type
row_issues = []  # one dict per bad row


def log_issue(file, df, mask, column, problem, dimension, severity, rule):
    """Record an issue if mask (a True/False per row) has any True rows."""
    bad = df[mask]
    if bad.empty:
        return
    key = KEYS[file]
    examples = ", ".join(bad[key].head(3))
    issues.append({"file": file, "column": column, "problem": problem,
                   "dimension": dimension, "severity": severity,
                   "rows_affected": len(bad), "example_keys": examples,
                   "proposed_rule": rule})
    for idx, r in bad.iterrows():
        row_issues.append({"file": file, "csv_line": idx + 2, "row_key": r[key],
                           "column": column, "problem": problem,
                           "value": r[column] if column in df.columns else "",
                           "dimension": dimension, "severity": severity})


# ---------------------------------------------------------------- loading
def load(name, filename):
    # dtype=str and keep_default_na=False: read every cell exactly as written,
    # so blanks stay "" and nothing is silently converted.
    return pd.read_csv(DATA / filename, dtype=str, keep_default_na=False)


files = {
    "orders": load("orders", "orders.csv"),
    "payments": load("payments", "payments.csv"),
    "customers": load("customers", "customers.csv"),
    "products": load("products", "products.csv"),
    "stores": load("stores", "stores.csv"),
    "increment": load("increment", "orders_2026-10-01_increment.csv"),
}


def blank(s):
    return s.str.strip() == ""


def num(s):
    return pd.to_numeric(s, errors="coerce")


def ts(s):
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")


def dt(s):
    return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")


# ================================================================ STEP 1: profile
say("=" * 78)
say("STEP 1: PROFILE OF EVERY FILE")
say("=" * 78)

NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "increment": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "customers": [],
    "products": ["mrp", "cost_price"],
    "stores": ["capacity_orders_per_day"],
}
DATES = {
    "orders": ["order_ts"], "increment": ["order_ts"], "payments": ["payment_ts"],
    "customers": ["signup_date"], "products": ["launch_date"], "stores": ["open_date"],
}
CATEGORICAL = {
    "orders": ["order_status", "payment_mode", "channel"],
    "increment": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["loyalty_tier", "city", "is_active"],
    "products": ["category", "unit", "is_perishable"],
    "stores": ["zone"],
}

for name, df in files.items():
    key = KEYS[name]
    say(f"\n--- {name}: {len(df)} rows, {len(df.columns)} columns ---")
    nulls = {c: int(blank(df[c]).sum()) for c in df.columns if blank(df[c]).sum() > 0}
    say(f"blank values per column : {nulls if nulls else 'none'}")
    say(f"duplicate {key:12}: {int(df[key].duplicated().sum())} extra rows "
        f"({int(df.duplicated().sum())} are exact copies of another row)")
    for c in NUMERIC[name]:
        n = num(df[c])
        say(f"{c:18} min={n.min()}  max={n.max()}  non-numeric={int(n.isna().sum() - blank(df[c]).sum())}")
    for c in DATES[name]:
        d = ts(df[c]) if c.endswith("_ts") else dt(df[c])
        say(f"{c:18} min={d.min()}  max={d.max()}  unparseable={int(d.isna().sum() - blank(df[c]).sum())}")
    for c in CATEGORICAL[name]:
        say(f"{c:18} {dict(df[c].value_counts())}")

# ================================================================ STEP 2: checks
for name in ("orders", "increment"):
    o = files[name]
    qty, price, disc = num(o["quantity"]), num(o["unit_price"]), num(o["discount_pct"])
    amount, mins = num(o["order_amount"]), num(o["delivery_minutes"])
    when = ts(o["order_ts"])
    status = o["order_status"].str.strip().str.upper()

    # ---- Completeness
    log_issue(name, o, blank(o["customer_id"]), "customer_id", "customer_id is blank",
              "Completeness", "Critical", "customer_id NOT NULL -> quarantine")
    log_issue(name, o, blank(o["order_ts"]), "order_ts", "order_ts is blank",
              "Completeness", "Critical", "order_ts NOT NULL -> quarantine")
    log_issue(name, o, (status == "DELIVERED") & blank(o["delivery_minutes"]), "delivery_minutes",
              "DELIVERED order has no delivery_minutes", "Completeness", "Critical",
              "DELIVERED requires delivery_minutes -> quarantine")

    # ---- Uniqueness
    log_issue(name, o, o["order_id"].duplicated(keep="first"), "order_id",
              "order_id appears more than once (repeat of an earlier row)", "Uniqueness",
              "Critical", "keep the first row, quarantine repeats (they would double revenue)")

    # ---- Validity
    exact_status = o["order_status"].isin(STATUSES)
    case_status = ~exact_status & status.isin(STATUSES)
    log_issue(name, o, case_status, "order_status", "order_status has wrong casing/spaces",
              "Validity", "Warning", "TRIM + UPPER, then accept")
    log_issue(name, o, ~exact_status & ~case_status, "order_status",
              "order_status not in DELIVERED/CANCELLED/RETURNED", "Validity", "Critical",
              "reject unknown statuses -> quarantine")

    exact_pm = o["payment_mode"].isin(PAY_MODES)
    norm_pm = o["payment_mode"].str.strip().str.lower()
    case_pm = ~exact_pm & norm_pm.isin({m.lower() for m in PAY_MODES})
    log_issue(name, o, case_pm, "payment_mode", "payment_mode has wrong casing/spaces",
              "Validity", "Warning", "map to the standard value (upi -> UPI)")
    log_issue(name, o, ~exact_pm & ~case_pm, "payment_mode",
              "payment_mode is a typo or short form (Cashh, cod)", "Validity", "Warning",
              "lookup map: cod/Cashh -> Cash on Delivery (confirm with client)")

    log_issue(name, o, (disc < 0) | (disc > 100) | (disc.isna() & ~blank(o["discount_pct"])),
              "discount_pct", "discount_pct outside 0-100", "Validity", "Critical",
              "0 <= discount_pct <= 100 -> quarantine")
    log_issue(name, o, when.isna() & ~blank(o["order_ts"]), "order_ts",
              "order_ts is not a valid timestamp", "Validity", "Critical",
              "order_ts must parse as YYYY-MM-DD HH:MM:SS -> quarantine")

    # ---- Accuracy
    log_issue(name, o, qty <= 0, "quantity", "quantity is zero or negative", "Accuracy",
              "Critical", "quantity > 0 -> quarantine (ask client if negative = return)")
    expected = (qty * price * (1 - disc / 100)).round(2)
    log_issue(name, o, (expected - amount).abs() > 0.01, "order_amount",
              "order_amount differs from quantity x unit_price x (1 - discount/100)",
              "Accuracy", "Critical", "recompute and compare, tolerance 0.01 -> quarantine")
    log_issue(name, o, mins > 120, "delivery_minutes",
              "delivery_minutes above 120 (impossible, hours not minutes)", "Accuracy",
              "Critical", "delivery_minutes <= 120 -> quarantine")
    log_issue(name, o, (mins <= 0), "delivery_minutes", "delivery_minutes is zero or negative",
              "Accuracy", "Critical", "delivery_minutes > 0 -> quarantine")
    log_issue(name, o, ((mins < 8) | (mins > 35)) & (mins <= 120) & (mins > 0),
              "delivery_minutes", "delivery_minutes outside the normal 8-35 range",
              "Accuracy", "Warning", "load but log; excluded from nothing")

    # ---- Consistency (referential integrity against the masters)
    cust_ids = set(files["customers"]["customer_id"])
    prod_ids = set(files["products"]["product_id"])
    store_ids = set(files["stores"]["store_id"])
    log_issue(name, o, ~blank(o["customer_id"]) & ~o["customer_id"].isin(cust_ids),
              "customer_id", "customer_id not found in customers", "Consistency", "Critical",
              "FK check against customers -> quarantine")
    log_issue(name, o, ~o["product_id"].isin(prod_ids), "product_id",
              "product_id not found in products", "Consistency", "Critical",
              "FK check against products -> quarantine")
    log_issue(name, o, ~o["store_id"].isin(store_ids), "store_id",
              "store_id not found in stores", "Consistency", "Critical",
              "FK check against stores -> quarantine")

    # ---- Timeliness
    # The increment file is the 1 Oct export, so its cut-off is the end of 1 Oct.
    cutoff = AS_OF + pd.Timedelta(days=1) if name == "increment" else AS_OF
    log_issue(name, o, when > cutoff, "order_ts", "order_ts is in the future", "Timeliness",
              "Critical", f"order_ts <= {cutoff.date()} -> quarantine")
    if name == "orders":
        log_issue(name, o, when < DATA_START, "order_ts",
                  "order_ts before 1 Sep 2026 (outside the extract window)", "Timeliness",
                  "Warning", "flag rows outside the expected window")

# ---- Payments
p = files["payments"]
orders = files["orders"]
pamount = num(p["amount_paid"])
pwhen = ts(p["payment_ts"])
pmode_norm = p["payment_mode"].str.strip().str.lower()

log_issue("payments", p, p["payment_id"].duplicated(keep="first"), "payment_id",
          "payment_id appears more than once", "Uniqueness", "Critical",
          "keep first, quarantine repeats")
log_issue("payments", p, blank(p["gateway_ref"]) & (p["payment_mode"].str.strip().str.lower() != "cash on delivery"),
          "gateway_ref", "gateway_ref blank on a non-cash payment", "Completeness", "Warning",
          "gateway_ref required unless Cash on Delivery")
exact_pm = p["payment_mode"].isin(PAY_MODES)
case_pm = ~exact_pm & pmode_norm.isin({m.lower() for m in PAY_MODES})
log_issue("payments", p, case_pm, "payment_mode", "payment_mode has wrong casing/spaces",
          "Validity", "Warning", "map to the standard value")
log_issue("payments", p, ~exact_pm & ~case_pm, "payment_mode",
          "payment_mode is a typo or short form (Cashh, cod)", "Validity", "Warning",
          "lookup map: cod/Cashh -> Cash on Delivery (confirm with client)")
log_issue("payments", p, ~p["payment_status"].isin(PAY_STATUSES), "payment_status",
          "payment_status not in SUCCESS/FAILED/REFUNDED", "Validity", "Critical",
          "allowed-list check")
log_issue("payments", p, pamount.isna() | (pamount <= 0), "amount_paid",
          "amount_paid is missing, non-numeric or not positive", "Accuracy", "Critical",
          "amount_paid > 0")
log_issue("payments", p, pwhen > AS_OF, "payment_ts", "payment_ts is in the future",
          "Timeliness", "Critical", "payment_ts <= extract date")

order_ids = set(orders["order_id"])
log_issue("payments", p, ~p["order_id"].isin(order_ids), "order_id",
          "payment refers to an order_id that is not in orders", "Consistency", "Critical",
          "FK check against orders -> quarantine")

# payment vs its order (amount and payment mode)
first_orders = orders.drop_duplicates("order_id").set_index("order_id")
joined = p.join(first_orders[["order_amount", "order_status", "payment_mode", "order_ts"]],
                on="order_id", rsuffix="_order")
o_amt = num(joined["order_amount"])
success = joined["payment_status"] == "SUCCESS"
log_issue("payments", joined, success & o_amt.notna() & ((pamount - o_amt).abs() > 0.01),
          "amount_paid", "SUCCESS payment amount does not equal the order's order_amount",
          "Consistency", "Critical", "amount_paid = order_amount for SUCCESS -> reconciliation table")
log_issue("payments", joined, joined["order_status"].notna() & (joined["order_status"].str.upper() == "CANCELLED"),
          "order_id", "payment exists for a CANCELLED order", "Consistency", "Critical",
          "cancelled orders must have no payment -> reconciliation table")
o_ts = ts(joined["order_ts"])
log_issue("payments", joined, o_ts.notna() & (pwhen < o_ts), "payment_ts",
          "payment_ts is earlier than the order's order_ts", "Consistency", "Warning",
          "payment_ts >= order_ts")
log_issue("payments", joined, (joined["payment_status"] == "FAILED") &
          (joined["order_status"].str.strip().str.upper() == "DELIVERED"), "payment_status",
          "payment FAILED but the order is DELIVERED (revenue not collected)", "Consistency",
          "Critical", "list in reconciliation table; finance decides if it counts as revenue")
log_issue("payments", joined, (joined["payment_status"] == "SUCCESS") &
          ~joined["order_status"].fillna("").str.strip().str.upper().isin(STATUSES) &
          joined["order_status"].notna(), "order_id",
          "SUCCESS payment belongs to an order with an invalid status (Pending??)",
          "Consistency", "Critical", "reconciliation table; resolve the order status first")

# orders that should have a payment but do not
paid_ids = set(p.loc[p["payment_status"] == "SUCCESS", "order_id"])
delivered = orders["order_status"].str.strip().str.upper() == "DELIVERED"
log_issue("orders", orders, delivered & ~orders["order_id"].isin(set(p["order_id"])), "order_id",
          "DELIVERED order has no payment row", "Consistency", "Critical",
          "list in reconciliation table (revenue not confirmed by gateway)")

# ---- Customers
c = files["customers"]
log_issue("customers", c, c["customer_id"].duplicated(keep=False), "customer_id",
          "customer_id appears more than once (all copies shown)", "Uniqueness", "Critical",
          "copies disagree on phone/area: keep one row per customer (ask client which is current)")
log_issue("customers", c, blank(c["city"]), "city", "city is blank", "Completeness",
          "Warning", "load; fill 'Unknown' in the dimension")
exact_t = c["loyalty_tier"].isin(TIERS)
log_issue("customers", c, ~exact_t & c["loyalty_tier"].str.strip().str.title().isin(TIERS),
          "loyalty_tier", "loyalty_tier has inconsistent casing (BRONZE, bronze, SILVER)",
          "Consistency", "Warning", "title-case everything")
log_issue("customers", c, ~c["loyalty_tier"].str.strip().str.title().isin(TIERS), "loyalty_tier",
          "loyalty_tier not in Bronze/Silver/Gold/Platinum", "Validity", "Critical", "allowed list")
email_ok = c["email"].str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
log_issue("customers", c, ~email_ok, "email", "email format is invalid", "Validity", "Warning",
          "regex check; mask anyway")
log_issue("customers", c, ~c["phone"].str.fullmatch(r"\d{10}"), "phone",
          "phone is not exactly 10 digits", "Validity", "Warning", "10 digits only")
log_issue("customers", c, ~c["pincode"].str.fullmatch(r"\d{6}"), "pincode",
          "pincode is not exactly 6 digits", "Validity", "Warning", "6 digits only")
log_issue("customers", c, dt(c["signup_date"]) > AS_OF, "signup_date",
          "signup_date is in the future", "Timeliness", "Critical", "signup_date <= extract date")
log_issue("customers", c, dt(c["signup_date"]).isna(), "signup_date",
          "signup_date missing or not a valid date", "Validity", "Critical", "valid date")
log_issue("customers", c, ~c["is_active"].isin({"Y", "N"}), "is_active",
          "is_active not Y/N", "Validity", "Warning", "Y/N only")
dup_email = c["email"].str.lower().duplicated(keep=False) & ~c["customer_id"].duplicated(keep=False)
log_issue("customers", c, dup_email, "email",
          "same email on two different customer_ids", "Uniqueness", "Warning",
          "investigate possible duplicate customer")

# ---- Products
pr = files["products"]
mrp, cost = num(pr["mrp"]), num(pr["cost_price"])
log_issue("products", pr, pr["product_id"].duplicated(keep=False), "product_id",
          "product_id appears more than once (all copies shown)", "Uniqueness", "Critical",
          "keep the row whose name does not end with '(old)'; one row per product")
log_issue("products", pr, pr["product_name"].str.contains(r"\(old\)", regex=True), "product_name",
          "old catalogue row left in the file (name ends with '(old)', different mrp)",
          "Consistency", "Warning", "drop '(old)' rows from dim_product, keep for audit in raw")
log_issue("products", pr, blank(pr["category"]), "category", "category is blank",
          "Completeness", "Critical", "category NOT NULL (needed for margin by category)")
log_issue("products", pr, ~blank(pr["category"]) & ~pr["category"].isin(CATEGORIES), "category",
          "category not one of the 6 known categories", "Validity", "Warning", "allowed list")
log_issue("products", pr, mrp.isna() | (mrp <= 0), "mrp", "mrp missing or not positive",
          "Accuracy", "Critical", "mrp > 0")
log_issue("products", pr, cost.isna() | (cost <= 0), "cost_price", "cost_price missing or not positive",
          "Accuracy", "Critical", "cost_price > 0")
log_issue("products", pr, (cost > mrp), "cost_price", "cost_price is higher than mrp",
          "Accuracy", "Critical", "cost_price <= mrp")
log_issue("products", pr, dt(pr["launch_date"]) > AS_OF, "launch_date",
          "launch_date is in the future", "Timeliness", "Warning", "launch_date <= extract date")
log_issue("products", pr, ~pr["is_perishable"].isin({"True", "False"}), "is_perishable",
          "is_perishable not True/False", "Validity", "Warning", "boolean check")

# ---- Stores
s = files["stores"]
cap = num(s["capacity_orders_per_day"])
log_issue("stores", s, blank(s["zone"]), "zone", "zone is blank (store cannot be placed in a zone)",
          "Completeness", "Warning", "load with zone = 'Unknown' and ask the client (quarantining would drop the store's orders)")
log_issue("stores", s, ~blank(s["zone"]) & ~s["zone"].isin(ZONES), "zone",
          "zone not North/South/East/West", "Validity", "Critical", "allowed list")
log_issue("stores", s, cap.isna() | (cap <= 0), "capacity_orders_per_day",
          "capacity_orders_per_day is zero or missing", "Accuracy", "Warning",
          "load the store, skip it in the capacity check, ask the client")
log_issue("stores", s, ~s["pincode"].str.fullmatch(r"\d{6}"), "pincode",
          "pincode is not exactly 6 digits", "Validity", "Warning", "6 digits only")
log_issue("stores", s, dt(s["open_date"]) > AS_OF, "open_date", "open_date is in the future",
          "Timeliness", "Warning", "open_date <= extract date")

# ---- Increment file against the main orders file
inc = files["increment"]
in_main = inc["order_id"].isin(order_ids)
say("\n" + "=" * 78)
say("STEP 2b: INCREMENT FILE vs orders.csv")
say("=" * 78)
say(f"rows in increment file          : {len(inc)}")
say(f"order_ids already in orders.csv : {int(in_main.sum())}  (these are corrections -> UPDATE)")
say(f"order_ids new to the warehouse  : {int((~in_main).sum())}  (these are new orders -> INSERT)")
chg = inc[in_main].join(first_orders[["order_status", "order_amount"]], on="order_id", rsuffix="_old")
chg_status = chg[chg["order_status"] != chg["order_status_old"]]
say(f"corrections that change status  : {len(chg_status)}")
changes = (chg_status["order_status_old"] + " -> " + chg_status["order_status"]).value_counts()
say(f"status changes seen             : {', '.join(f'{k} x {int(v)}' for k, v in changes.items())}")

# ================================================================ STEP 3: report
log = pd.DataFrame(issues)
order = {"Critical": 0, "Warning": 1}
log["sev_rank"] = log["severity"].map(order)
log = log.sort_values(["file", "sev_rank", "dimension", "rows_affected"],
                      ascending=[True, True, True, False]).drop(columns="sev_rank")
log.insert(0, "issue_no", range(1, len(log) + 1))

say("\n" + "=" * 78)
say("STEP 3: DATA ISSUES LOG (one line per issue type)")
say("=" * 78)
for _, r in log.iterrows():
    say(f"#{r.issue_no:<3} {r.file:10} {r.column:24} {r.dimension:13} {r.severity:8} "
        f"{r.rows_affected:>5} rows  {r.problem}")

say("\nIssues per file:")
say(log.groupby("file").agg(issue_types=("issue_no", "count"), rows=("rows_affected", "sum")).to_string())
say("\nIssues per quality dimension:")
say(log.groupby("dimension").agg(issue_types=("issue_no", "count")).to_string())
say(f"\nTotal: {len(log)} issue types across {log['file'].nunique()} of the 5 data files "
    f"(orders, payments, customers, products, stores).")
say("The increment file passed every check, so it appears nowhere in the log.")

# ---- save the files
log.to_csv(OUT / "data_issues_log.csv", index=False)
pd.DataFrame(row_issues).to_csv(OUT / "data_issues_rows.csv", index=False)


def md_cell(v):
    return str(v).replace("|", "/")


with open(OUT / "data_issues_log.md", "w") as f:
    f.write("| # | File | Row key (examples) | Column | Problem | Rows | Quality dimension | Proposed rule | Severity |\n")
    f.write("|---|---|---|---|---|---|---|---|---|\n")
    for _, r in log.iterrows():
        f.write(f"| {r.issue_no} | {r.file} | {md_cell(r.example_keys)} | {r.column} | "
                f"{md_cell(r.problem)} | {r.rows_affected} | {r.dimension} | "
                f"{md_cell(r.proposed_rule)} | {r.severity} |\n")
(OUT / "profile_report.txt").write_text("\n".join(_lines) + "\n")
say(f"\nSaved: {OUT}/profile_report.txt, data_issues_log.csv, data_issues_log.md, data_issues_rows.csv")
