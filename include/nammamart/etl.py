"""
ETL pipeline logic (Part A). dags/nammamart_etl_saksham.py only wires these functions together.

    extract (parallel) -> validate -> quarantine -> transform -> load -> reconcile -> report -> audit

Files move between tasks through a per-run scratch folder (include/stage/saksham/<run_id>/),
and only small summaries travel through Airflow's XCom.
"""
import re

import pandas as pd

from nammamart import checks, masking
from nammamart import config as cfg
from nammamart import warehouse as wh

DAG_ID = "nammamart_etl_saksham"


class PermanentSchemaError(Exception):
    """A required column is missing. Retrying will not fix it, so the DAG does not retry."""


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)


def stage_dir(run_id: str):
    d = cfg.STAGE_DIR / safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def read_text_csv(path) -> pd.DataFrame:
    """Read every cell as text and keep blanks as empty strings (nothing converted silently)."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


# ================================================================== EXTRACT
def extract(key: str, run_id: str, enabled: bool = True) -> dict:
    if not enabled:
        print(f"[Extract] {key}: not requested for this run, skipped")
        return {"file": key, "rows": 0, "skipped": True}
    path = cfg.DATA_DIR / cfg.FILES[key]
    df = read_text_csv(path)                      # a missing file raises FileNotFoundError
    missing = [c for c in cfg.REQUIRED_COLUMNS[key] if c not in df.columns]
    if missing:
        raise PermanentSchemaError(f"{cfg.FILES[key]} is missing required column(s): {missing}")
    df.to_csv(stage_dir(run_id) / f"raw_{key}.csv", index=False)
    print(f"[Extract] {key}: {len(df)} rows read from {path.name}")
    return {"file": key, "rows": int(len(df)), "skipped": False}


# ================================================================== VALIDATE
def validate(extracts: list, run_id: str, as_of: str, load_increment: bool) -> dict:
    sd = stage_dir(run_id)
    raw = {e["file"]: read_text_csv(sd / f"raw_{e['file']}.csv")
           for e in extracts if not e.get("skipped")}

    # What the checks need to know about the other files
    products = raw["products"].assign(_old=raw["products"]["product_name"].str.contains(r"\(old\)", regex=True))
    kept_products = products.sort_values("_old", kind="stable").drop_duplicates("product_id")
    orders_ref = (raw["orders"].drop_duplicates("order_id").set_index("order_id")
                  [["order_amount", "order_status", "order_ts"]].add_suffix("_o"))
    previous = None
    try:
        con = wh.connect()
        previous = wh.last_check_rows(con, DAG_ID, "orders.volume_vs_last_run", run_id)
        con.close()
    except Exception as err:                       # first run, no history yet
        print(f"[Validate] no earlier run to compare volume with ({err})")
    ctx = checks.Context(
        as_of=as_of,
        master_ids={"customers": set(raw["customers"]["customer_id"]),
                    "products": set(raw["products"]["product_id"]),
                    "stores": set(raw["stores"]["store_id"]),
                    "orders": set(raw["orders"]["order_id"])},
        orders_ref=orders_ref,
        product_mrp=kept_products.set_index("product_id")["mrp"],
        store_capacity=raw["stores"].drop_duplicates("store_id").set_index("store_id")["capacity_orders_per_day"],
        previous_orders_volume=previous)

    runners = {
        "customers": lambda df: checks.validate_customers(df, ctx),
        "products": lambda df: checks.validate_products(df, ctx),
        "stores": lambda df: checks.validate_stores(df, ctx),
        "orders": lambda df: checks.validate_orders(df, "orders", ctx),
        "increment": lambda df: checks.validate_orders(df, "increment", ctx),
        "payments": lambda df: checks.validate_payments(df, ctx),
    }
    all_results, counts = [], {}
    for name, run_checks in runners.items():
        if name not in raw:
            continue
        df = raw[name]
        results, failed, reasons = run_checks(df)
        passed_df = df[~failed]
        failed_df = df[failed].assign(failure_reason=reasons[failed])
        passed_df.to_csv(sd / f"passed_{name}.csv", index=False)
        failed_df.to_csv(sd / f"failed_{name}.csv", index=False)
        counts[name] = {"in": int(len(df)), "passed": int(len(passed_df)), "failed": int(len(failed_df))}
        all_results.extend(results)
        print(f"[Validate] {name}: {len(df)} in, {len(passed_df)} passed, {len(failed_df)} quarantined")
    warn = sum(1 for r in all_results if r["status"] == "WARN")
    fail = sum(1 for r in all_results if r["status"] == "FAIL")
    print(f"[Validate] {len(all_results)} checks run: {fail} Critical failing, {warn} Warning failing")
    return {"checks": all_results, "counts": counts}


# ================================================================== QUARANTINE
def mask_pii(df: pd.DataFrame) -> pd.DataFrame:
    """Quarantine files live in the repo folder, so personal data is masked there too."""
    out = df.copy()
    if "email" in out:
        out["email"] = out["email"].map(masking.mask_email)
    if "phone" in out:
        out["phone"] = out["phone"].map(masking.mask_phone)
    if "full_name" in out:
        out["full_name"] = out["full_name"].map(masking.mask_name)
    return out


def quarantine(val: dict, run_id: str) -> dict:
    sd = stage_dir(run_id)
    cfg.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    log_rows, files, total = [], {}, 0
    stamp = wh.now_ist()
    for name, cnt in val["counts"].items():
        if cnt["failed"] == 0:
            continue
        failed = read_text_csv(sd / f"failed_{name}.csv")
        path = cfg.QUARANTINE_DIR / f"{name}_failed_{safe_run_id(run_id)}.csv"
        mask_pii(failed).to_csv(path, index=False)
        files[name] = str(path)
        total += len(failed)
        key = cfg.KEYS[name]
        for _, row in failed.iterrows():
            for reason in row["failure_reason"].split("; "):
                log_rows.append({"run_id": run_id, "file": name, "row_key": row[key],
                                 "failure_reason": reason, "logged_at": stamp})
        print(f"[Quarantine] {len(failed)} {name} rows written to {path.name}")
    if total == 0:
        print("[Quarantine] No rows failed a Critical check.")

    con = wh.connect()
    wh.write_dq_results(con, run_id, DAG_ID, val["checks"])
    con.execute("""CREATE TABLE IF NOT EXISTS etl_quarantine_log (
        run_id VARCHAR, file VARCHAR, row_key VARCHAR, failure_reason VARCHAR, logged_at TIMESTAMP)""")
    con.execute("DELETE FROM etl_quarantine_log WHERE run_id = ?", [run_id])
    if log_rows:
        log_df = pd.DataFrame(log_rows)
        con.register("qlog", log_df)
        con.execute("INSERT INTO etl_quarantine_log SELECT * FROM qlog")
        con.unregister("qlog")
    con.close()
    return {"quarantined": total, "files": files, "per_file": {k: v["failed"] for k, v in val["counts"].items()}}


# ================================================================== TRANSFORM
FACT_COLUMNS = ["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
                "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
                "order_status", "channel", "delivery_minutes", "net_revenue", "gross_margin",
                "within_15_min", "source_file", "run_id"]


def build_fact(df: pd.DataFrame, cost: pd.Series, source_file: str, run_id: str) -> pd.DataFrame:
    """Standardise one passed orders file and add net_revenue, gross_margin, within_15_min."""
    f = pd.DataFrame({
        "order_id": df["order_id"].str.strip(),
        "order_ts": checks.ts(df["order_ts"]),
        "customer_id": df["customer_id"].str.strip(),
        "store_id": df["store_id"].str.strip(),
        "product_id": df["product_id"].str.strip(),
        "quantity": checks.num(df["quantity"]).astype("int64"),
        "unit_price": checks.num(df["unit_price"]),
        "discount_pct": checks.num(df["discount_pct"]),
        "order_amount": checks.num(df["order_amount"]).round(2),
        "payment_mode": df["payment_mode"].str.strip().str.lower().map(cfg.PAYMENT_MODE_MAP),
        "order_status": df["order_status"].str.strip().str.upper(),
        "channel": df["channel"].str.strip(),
        "delivery_minutes": checks.num(df["delivery_minutes"]),
    })
    f["order_date"] = f["order_ts"].dt.date
    delivered = f["order_status"] == "DELIVERED"
    unit_cost = f["product_id"].map(cost)
    f["net_revenue"] = f["order_amount"].where(delivered, 0.0)
    # margin = revenue - quantity x cost_price (empty when the product's cost is unknown)
    f["gross_margin"] = (f["order_amount"] - f["quantity"] * unit_cost).where(delivered, 0.0)
    f["within_15_min"] = delivered & (f["delivery_minutes"] <= 15)
    f["source_file"] = source_file
    f["run_id"] = run_id
    return f[FACT_COLUMNS]


def transform(val: dict, run_id: str, load_increment: bool) -> dict:
    sd = stage_dir(run_id)
    get = lambda n: read_text_csv(sd / f"passed_{n}.csv")

    # Customers: standardise, mask personal data, no raw name/email/phone in reporting tables
    cust = get("customers")
    dim_customer = pd.DataFrame({
        "customer_id": cust["customer_id"],
        "customer_name_masked": cust["full_name"].map(masking.mask_name),
        "email_masked": cust["email"].map(masking.mask_email),
        "phone_masked": cust["phone"].map(masking.mask_phone),
        "city": cust["city"].str.strip().replace("", "Unknown"),
        "area": cust["area"],
        "pincode": cust["pincode"],
        "signup_date": checks.dt(cust["signup_date"]).dt.date,
        "loyalty_tier": cust["loyalty_tier"].str.strip().str.title(),
        "is_active": cust["is_active"],
    })
    prod = get("products")
    dim_product = pd.DataFrame({
        "product_id": prod["product_id"], "product_name": prod["product_name"],
        "category": prod["category"], "sub_category": prod["sub_category"], "brand": prod["brand"],
        "mrp": checks.num(prod["mrp"]), "cost_price": checks.num(prod["cost_price"]),
        "unit": prod["unit"], "is_perishable": prod["is_perishable"] == "True",
        "launch_date": checks.dt(prod["launch_date"]).dt.date})
    st = get("stores")
    dim_store = pd.DataFrame({
        "store_id": st["store_id"], "store_name": st["store_name"],
        "zone": st["zone"].str.strip().replace("", "Unknown"), "area": st["area"],
        "pincode": st["pincode"], "open_date": checks.dt(st["open_date"]).dt.date,
        "manager_name": st["manager_name"],
        "capacity_orders_per_day": checks.num(st["capacity_orders_per_day"]).astype("int64")})

    cost = dim_product.set_index("product_id")["cost_price"]
    fact_base = build_fact(get("orders"), cost, "orders.csv", run_id)
    fact_inc = None
    if load_increment and "increment" in val["counts"]:
        fact_inc = build_fact(get("increment"), cost, cfg.FILES["increment"], run_id)

    dim_customer.to_pickle(sd / "dim_customer.pkl")
    dim_product.to_pickle(sd / "dim_product.pkl")
    dim_store.to_pickle(sd / "dim_store.pkl")
    fact_base.to_pickle(sd / "fact_base.pkl")
    if fact_inc is not None:
        fact_inc.to_pickle(sd / "fact_increment.pkl")
    out = {"dim_customer": len(dim_customer), "dim_product": len(dim_product),
           "dim_store": len(dim_store), "fact_base": len(fact_base),
           "fact_increment": 0 if fact_inc is None else len(fact_inc)}
    print(f"[Transform] {out}")
    return out


# ================================================================== LOAD
FACT_DDL = """CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, store_id VARCHAR,
    product_id VARCHAR, quantity BIGINT, unit_price DOUBLE, discount_pct DOUBLE,
    order_amount DOUBLE, payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes DOUBLE, net_revenue DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN,
    source_file VARCHAR, run_id VARCHAR)"""

SCD2_DDL = """CREATE TABLE IF NOT EXISTS etl_dim_customer_scd2 (
    customer_id VARCHAR, loyalty_tier VARCHAR, city VARCHAR, area VARCHAR, pincode VARCHAR,
    is_active VARCHAR, valid_from DATE, valid_to DATE, is_current BOOLEAN)"""
SCD2_TRACKED = ["loyalty_tier", "city", "area", "pincode", "is_active"]


def upsert_fact(con, df: pd.DataFrame) -> tuple:
    """Delete-then-insert by order_id. Returns (inserted, updated). Running it twice changes nothing."""
    if df.empty:
        return 0, 0
    con.register("incoming_fact", df)
    updated = con.execute("SELECT COUNT(*) FROM etl_fact_orders WHERE order_id IN "
                          "(SELECT order_id FROM incoming_fact)").fetchone()[0]
    con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM incoming_fact)")
    cols = ", ".join(FACT_COLUMNS)
    con.execute(f"INSERT INTO etl_fact_orders ({cols}) SELECT {cols} FROM incoming_fact")
    con.unregister("incoming_fact")
    return len(df) - updated, updated


def load(val_counts: dict, run_id: str, load_increment: bool) -> dict:
    sd = stage_dir(run_id)
    con = wh.connect()
    wh.ensure_audit_tables(con)
    con.execute(FACT_DDL)
    con.execute(SCD2_DDL)
    run_date = wh.now_ist().date()
    metrics = {}
    try:
        con.execute("BEGIN")
        # Dimensions are small, so they are rebuilt from the clean rows each run (same result every time)
        for name in ("dim_customer", "dim_product", "dim_store"):
            df = pd.read_pickle(sd / f"{name}.pkl")
            con.register("dim_df", df)
            con.execute(f"CREATE OR REPLACE TABLE etl_{name} AS SELECT * FROM dim_df")
            con.unregister("dim_df")
            metrics[name] = len(df)

        # Bonus: SCD Type 2 history of customers (tier, city, area, pincode, active flag)
        incoming = pd.read_pickle(sd / "dim_customer.pkl")[["customer_id"] + SCD2_TRACKED]
        current = con.execute("SELECT customer_id, " + ", ".join(SCD2_TRACKED) +
                              " FROM etl_dim_customer_scd2 WHERE is_current").df()
        merged = incoming.merge(current, on="customer_id", how="left", suffixes=("", "_old"), indicator=True)
        is_new = merged["_merge"] == "left_only"
        differs = pd.Series(False, index=merged.index)
        for col in SCD2_TRACKED:
            differs |= merged[col] != merged[col + "_old"]
        changed = (merged["_merge"] == "both") & differs
        if changed.any():
            con.register("closing", merged.loc[changed, ["customer_id"]])
            con.execute("UPDATE etl_dim_customer_scd2 SET valid_to = ?, is_current = FALSE "
                        "WHERE is_current AND customer_id IN (SELECT customer_id FROM closing)", [run_date])
            con.unregister("closing")
        new_versions = merged.loc[is_new | changed, ["customer_id"] + SCD2_TRACKED].assign(
            valid_from=run_date, valid_to=pd.Timestamp("9999-12-31").date(), is_current=True)
        if len(new_versions):
            con.register("versions", new_versions)
            con.execute("INSERT INTO etl_dim_customer_scd2 SELECT customer_id, loyalty_tier, city, area, "
                        "pincode, is_active, valid_from, valid_to, is_current FROM versions")
            con.unregister("versions")
        metrics["scd2_new_customers"] = int(is_new.sum())
        metrics["scd2_changed_customers"] = int(changed.sum())

        # Fact: base file first, then the increment (which wins for any order_id it contains).
        base = pd.read_pickle(sd / "fact_base.pkl")
        protected = con.execute("SELECT order_id FROM etl_fact_orders WHERE source_file <> 'orders.csv'").df()
        base = base[~base["order_id"].isin(set(protected["order_id"]))]   # never undo an earlier correction
        ins, upd = upsert_fact(con, base)
        metrics.update({"base_inserted": ins, "base_updated": upd})
        metrics["increment_inserted"] = metrics["increment_updated"] = 0
        inc_path = sd / "fact_increment.pkl"
        if load_increment and inc_path.exists():
            ins2, upd2 = upsert_fact(con, pd.read_pickle(inc_path))
            metrics.update({"increment_inserted": ins2, "increment_updated": upd2})
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    metrics["fact_total"] = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    metrics["rows_loaded"] = (metrics["base_inserted"] + metrics["base_updated"]
                              + metrics["increment_inserted"] + metrics["increment_updated"])
    con.close()
    print(f"[Load] {metrics}")
    return metrics


# ================================================================== RECONCILE
def reconcile(run_id: str, load_increment: bool) -> dict:
    sd = stage_dir(run_id)
    pay = read_text_csv(sd / "raw_payments.csv").drop_duplicates("payment_id")
    pay["amount"] = checks.num(pay["amount_paid"])
    raw_order_ids = set(read_text_csv(sd / "raw_orders.csv")["order_id"])

    con = wh.connect()
    fact = con.execute("SELECT order_id, order_status, order_amount FROM etl_fact_orders").df()
    success = pay[pay["payment_status"] == "SUCCESS"].groupby("order_id")["amount"].sum()
    success_id = pay[pay["payment_status"] == "SUCCESS"].groupby("order_id")["payment_id"].first()
    failed_ids = set(pay.loc[pay["payment_status"] == "FAILED", "order_id"])
    failed_id = pay[pay["payment_status"] == "FAILED"].groupby("order_id")["payment_id"].first()
    refunded = set(pay.loc[pay["payment_status"] == "REFUNDED", "order_id"])
    any_pay = set(pay["order_id"])

    rows = []

    def add(order_id, payment_id, kind, status, pstatus, order_amt, paid, reason):
        rows.append({"run_id": run_id, "order_id": order_id, "payment_id": payment_id,
                     "mismatch_type": kind, "order_status": status, "payment_status": pstatus,
                     "order_amount": order_amt, "amount_paid": paid,
                     "difference": round((order_amt or 0.0) - (paid or 0.0), 2), "reason": reason})

    for o in fact.itertuples(index=False):
        paid = float(success.get(o.order_id, 0.0))
        if o.order_status == "DELIVERED":
            if o.order_id not in any_pay:
                add(o.order_id, None, "DELIVERED_NO_PAYMENT", "DELIVERED", None, o.order_amount, 0.0,
                    "Delivered order has no payment row in the gateway export")
            elif o.order_id not in success.index:
                add(o.order_id, failed_id.get(o.order_id), "DELIVERED_PAYMENT_FAILED", "DELIVERED",
                    "FAILED", o.order_amount, 0.0, "Delivered order, but the only payment attempt FAILED")
            elif abs(paid - o.order_amount) > cfg.AMOUNT_TOLERANCE:
                add(o.order_id, success_id.get(o.order_id), "AMOUNT_MISMATCH", "DELIVERED",
                    "SUCCESS", o.order_amount, paid, "SUCCESS payment differs from order_amount")
        elif o.order_status == "CANCELLED" and o.order_id in success.index:
            add(o.order_id, success_id.get(o.order_id), "CANCELLED_HAS_PAYMENT", "CANCELLED",
                "SUCCESS", o.order_amount, paid, "Cancelled order still has a SUCCESS payment")
        elif o.order_status == "RETURNED" and o.order_id in success.index and o.order_id not in refunded:
            add(o.order_id, success_id.get(o.order_id), "RETURNED_NOT_REFUNDED", "RETURNED",
                "SUCCESS", o.order_amount, paid, "Returned order was paid but no refund is recorded")

    in_fact = set(fact["order_id"])
    for p in pay[~pay["order_id"].isin(in_fact)].itertuples(index=False):
        reason = ("Order is not in the orders file" if p.order_id not in raw_order_ids
                  else "Order was quarantined, so it is not in the fact table")
        add(p.order_id, p.payment_id, "PAYMENT_WITHOUT_ORDER", None, p.payment_status, None,
            p.amount if pd.notna(p.amount) else 0.0, reason)

    cols = ["run_id", "order_id", "payment_id", "mismatch_type", "order_status", "payment_status",
            "order_amount", "amount_paid", "difference", "reason"]
    rec = pd.DataFrame(rows, columns=cols)
    con.register("rec_df", rec)
    con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM rec_df")
    con.unregister("rec_df")
    con.close()
    summary = rec.groupby("mismatch_type").size().to_dict()
    print(f"[Reconcile] {len(rec)} mismatches: {summary}")
    return {"mismatches": int(len(rec)), "by_type": {k: int(v) for k, v in summary.items()}}


# ================================================================== REPORT
REPORT_SQL = [
    ("etl_daily_zone_revenue", """
        SELECT f.order_date, COALESCE(s.zone, 'Unknown') AS zone, f.store_id, s.store_name,
               COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
               ROUND(SUM(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f LEFT JOIN etl_dim_store s ON s.store_id = f.store_id
        GROUP BY f.order_date, COALESCE(s.zone, 'Unknown'), f.store_id, s.store_name"""),
    ("etl_category_margin", """
        SELECT COALESCE(p.category, 'Unknown') AS category,
               ROUND(SUM(f.net_revenue), 2) AS revenue,
               ROUND(SUM(f.gross_margin), 2) AS gross_margin,
               ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 2) AS margin_pct
        FROM etl_fact_orders f LEFT JOIN etl_dim_product p ON p.product_id = f.product_id
        GROUP BY 1"""),
    ("etl_store_delivery_sla", """
        SELECT f.store_id, s.store_name,
               ROUND(AVG(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
               COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
               ROUND(100.0 * COUNT(*) FILTER (WHERE f.within_15_min) /
                     NULLIF(COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED'), 0), 1) AS pct_within_15_min
        FROM etl_fact_orders f LEFT JOIN etl_dim_store s ON s.store_id = f.store_id
        GROUP BY f.store_id, s.store_name"""),
    ("etl_revenue_by_payment_mode", """
        SELECT payment_mode, ROUND(SUM(net_revenue), 2) AS revenue,
               COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders
        FROM etl_fact_orders GROUP BY payment_mode"""),
    ("etl_top_customers", """
        SELECT f.customer_id, c.customer_name_masked, c.loyalty_tier,
               ROUND(SUM(f.net_revenue), 2) AS total_spend,
               COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders
        FROM etl_fact_orders f LEFT JOIN etl_dim_customer c ON c.customer_id = f.customer_id
        GROUP BY f.customer_id, c.customer_name_masked, c.loyalty_tier
        ORDER BY total_spend DESC LIMIT 10"""),
]


def report(run_id: str) -> dict:
    con = wh.connect()
    for name, sql in REPORT_SQL:
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.max_rows", 40)
    print("\n[Report] Q1 daily net revenue by zone (first 12 rows of etl_daily_zone_revenue)")
    print(con.execute("SELECT order_date, zone, store_id, delivered_orders, net_revenue "
                      "FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id LIMIT 12").df().to_string(index=False))
    print("\n[Report] Q2 revenue and gross margin by category (etl_category_margin)")
    print(con.execute("SELECT * FROM etl_category_margin ORDER BY revenue DESC").df().to_string(index=False))
    print("\n[Report] Q4 revenue by payment mode (etl_revenue_by_payment_mode)")
    print(con.execute("SELECT * FROM etl_revenue_by_payment_mode ORDER BY revenue DESC").df().to_string(index=False))
    unrec = con.execute("SELECT ROUND(SUM(ABS(difference)), 2) FROM etl_payment_reconciliation").fetchone()[0]
    print(f"[Report] Q4 total amount that does not reconcile with the gateway: {unrec}")
    print("\n[Report] Q5 top 10 customers (etl_top_customers)")
    print(con.execute("SELECT * FROM etl_top_customers").df().to_string(index=False))
    print("\n[Report] Q6 rows rejected in this run, by file and reason (etl_quarantine_log)")
    print(con.execute("SELECT file, failure_reason, COUNT(*) AS rows FROM etl_quarantine_log "
                      "WHERE run_id = ? GROUP BY 1, 2 ORDER BY 1, 3 DESC", [run_id]).df().to_string(index=False))
    total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
    con.close()
    print(f"\n[Report] total net revenue in etl_fact_orders: {total}")
    return {"total_net_revenue": float(total or 0.0)}


# ================================================================== AUDIT
def audit(run_id: str, start_time, extract_results: list, quarantine_result, load_result, status: str) -> dict:
    extracted = sum(int(e.get("rows", 0)) for e in (extract_results or []) if isinstance(e, dict))
    row = {"run_id": run_id, "dag_id": DAG_ID, "start_time": wh.to_ist(start_time),
           "end_time": wh.now_ist(), "rows_extracted": extracted,
           "rows_loaded": int((load_result or {}).get("rows_loaded", 0)),
           "rows_quarantined": int((quarantine_result or {}).get("quarantined", 0)),
           "status": status}
    con = wh.connect()
    wh.write_pipeline_run(con, row)
    con.close()
    print(f"[Audit] {row}")
    return {k: str(v) for k, v in row.items()}
