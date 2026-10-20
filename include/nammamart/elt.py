"""
ELT pipeline logic (Part B). dags/nammamart_elt_saksham.py only wires these functions together.

    load raw (parallel, untouched) -> stage with SQL (checks, typing, masking) -> MERGE increment
        -> marts with SQL -> compare with ETL -> report -> audit

Every transformation is a .sql file in include/sql/. Python only fills in {{placeholders}},
runs the file and prints results.
"""
import csv
import re

from nammamart import config as cfg
from nammamart import warehouse as wh
from nammamart.etl import PermanentSchemaError, safe_run_id

DAG_ID = "nammamart_elt_saksham"

RAW_TABLE = {"orders": "raw_orders", "increment": "raw_orders", "payments": "raw_payments",
             "customers": "raw_customers", "products": "raw_products", "stores": "raw_stores"}

# staging entity -> (check file, staging files, raw table the check reads)
STAGES = {
    "customers": (["10_chk_customers.sql", "20_stg_customers.sql"], "raw_customers"),
    "products": (["11_chk_products.sql", "21_stg_products.sql"], "raw_products"),
    "stores": (["12_chk_stores.sql", "22_stg_stores.sql"], "raw_stores"),
    "orders": (["13_chk_orders.sql", "23_stg_orders.sql", "24_stg_orders_increment.sql"], "raw_orders"),
    "payments": (["14_chk_payments.sql", "26_stg_payments.sql"], "raw_payments"),
}
MARTS = {
    "daily_store_kpis": "41_mart_daily_store_kpis.sql",
    "category_margin": "42_mart_category_margin.sql",
    "delivery_sla": "43_mart_delivery_sla.sql",
    "payment_reconciliation": "44_mart_payment_reconciliation.sql",
    "top_customers": "45_mart_top_customers.sql",
    "revenue_by_payment_mode": "46_mart_revenue_by_payment_mode.sql",
}


def render(sql: str, params: dict) -> str:
    for key, value in params.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    left = re.findall(r"\{\{(\w+)\}\}", sql)
    if left:
        raise ValueError(f"unfilled placeholder(s) in SQL: {sorted(set(left))}")
    return sql


def statements(sql: str):
    """Split a .sql file into single statements. Comment lines are dropped, and a semicolon
    inside a quoted string (for example '; ') does not end a statement."""
    body = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    out, current, in_quote = [], [], False
    for ch in body:
        if ch == "'":
            in_quote = not in_quote
        if ch == ";" and not in_quote:
            out.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    out.append("".join(current).strip())
    return [s for s in out if s]


def run_files(con, files, run_id, as_of=cfg.DEFAULT_AS_OF, **extra):
    params = {"run_id": safe_run_id(run_id), "as_of": as_of, "dag_id": DAG_ID,
              "now": wh.now_ist().strftime("%Y-%m-%d %H:%M:%S")}
    params.update(extra)
    for name in files:
        text = render((cfg.SQL_DIR / name).read_text(), params)
        for stmt in statements(text):
            con.execute(stmt)
        print(f"[SQL] ran {name}")


# ================================================================== RAW
def load_raw(key: str, run_id: str, enabled: bool = True) -> dict:
    if not enabled:
        print(f"[Raw] {key}: not requested for this run, skipped")
        return {"file": key, "rows": 0, "skipped": True}
    path = cfg.DATA_DIR / cfg.FILES[key]
    with open(path, newline="", encoding="utf-8") as fh:        # a missing file raises FileNotFoundError
        header = next(csv.reader(fh))
    missing = [c for c in cfg.REQUIRED_COLUMNS[key] if c not in header]
    if missing:
        raise PermanentSchemaError(f"{cfg.FILES[key]} is missing required column(s): {missing}")
    if header != cfg.REQUIRED_COLUMNS[key]:
        raise PermanentSchemaError(f"{cfg.FILES[key]} columns {header} are not in the expected order "
                                   f"{cfg.REQUIRED_COLUMNS[key]}")
    con = wh.connect()
    wh.ensure_audit_tables(con)
    run_files(con, ["01_raw_load.sql"], run_id, table=RAW_TABLE[key], path=path.as_posix(),
              source_file=cfg.FILES[key])
    rows = con.execute(f"SELECT COUNT(*) FROM {RAW_TABLE[key]} WHERE _run_id = ? AND _source_file = ?",
                       [safe_run_id(run_id), cfg.FILES[key]]).fetchone()[0]
    total = con.execute(f"SELECT COUNT(*) FROM {RAW_TABLE[key]}").fetchone()[0]
    con.close()
    print(f"[Raw] {key}: {rows} rows appended to {RAW_TABLE[key]} (table now holds {total} rows)")
    return {"file": key, "rows": int(rows), "skipped": False}


# ================================================================== STAGING
def stage(entity: str, run_id: str, as_of: str) -> dict:
    files, raw = STAGES[entity]
    con = wh.connect()
    run_files(con, files, run_id, as_of, raw=raw)
    out = counts(con, entity, run_id)
    con.close()
    print(f"[Stage] {entity}: {out}")
    return out


def counts(con, entity: str, run_id: str) -> dict:
    tables = ["stg_orders", "stg_orders_increment"] if entity == "orders" else [f"stg_{entity}"]
    res = {}
    for t in tables:
        passed, quarantined = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE dq_status = 'PASSED'), "
            f"COUNT(*) FILTER (WHERE dq_status = 'QUARANTINED') FROM {t}").fetchone()
        res[t] = {"passed": int(passed), "quarantined": int(quarantined)}
    return res


def merge_increment(run_id: str, as_of: str) -> dict:
    con = wh.connect()
    before = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    run_files(con, ["25_merge_orders_increment.sql"], run_id, as_of)
    after = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    inc = con.execute("SELECT COUNT(*) FROM stg_orders_increment WHERE dq_status = 'PASSED'").fetchone()[0]
    updated = con.execute("SELECT COUNT(*) FROM stg_orders t JOIN stg_orders_increment s "
                          "ON t.order_id = s.order_id AND s.dq_status = 'PASSED' AND t.dq_status = 'PASSED' "
                          "WHERE t._source_file = s._source_file").fetchone()[0]
    con.close()
    out = {"stg_orders_before": int(before), "stg_orders_after": int(after),
           "increment_rows_passed": int(inc), "rows_from_increment_in_stg": int(updated),
           "new_orders_inserted": int(after - before)}
    print(f"[Merge] {out}")
    return out


def dq_results(run_id: str, as_of: str) -> dict:
    con = wh.connect()
    run_files(con, ["30_dq_results.sql"], run_id, as_of)
    row = con.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'FAIL'), "
                      "COUNT(*) FILTER (WHERE status = 'WARN') FROM dq_results "
                      "WHERE run_id = ? AND dag_id = ?", [safe_run_id(run_id), DAG_ID]).fetchone()
    con.close()
    print(f"[DQ] {row[0]} checks written to dq_results: {row[1]} Critical failing, {row[2]} Warning failing")
    return {"checks": int(row[0]), "critical_failing": int(row[1]), "warning_failing": int(row[2])}


# ================================================================== MARTS
def build_fact_view(run_id: str, as_of: str):
    con = wh.connect()
    run_files(con, ["40_elt_fact_orders.sql", "49_elt_rejected_rows.sql"], run_id, as_of)
    con.close()


def build_mart(name: str, run_id: str, as_of: str) -> dict:
    con = wh.connect()
    run_files(con, [MARTS[name]], run_id, as_of)
    n = con.execute(f"SELECT COUNT(*) FROM mart_{name}").fetchone()[0]
    con.close()
    print(f"[Mart] mart_{name}: {n} rows")
    return {"table": f"mart_{name}", "rows": int(n)}


def check_totals(run_id: str, as_of: str) -> dict:
    con = wh.connect()
    has_etl = con.execute("SELECT COUNT(*) FROM information_schema.tables "
                          "WHERE table_name IN ('etl_fact_orders', 'etl_payment_reconciliation')").fetchone()[0] == 2
    if not has_etl:
        con.close()
        print("[Compare] The ETL tables do not exist yet, so there is nothing to compare with. Run the ETL DAG first.")
        return {"compared": False}
    run_files(con, ["47_check_totals_vs_etl.sql"], run_id, as_of)
    df = con.execute("SELECT * FROM elt_vs_etl_totals").df()
    con.close()
    print("[Compare] ELT marts vs ETL tables\n" + df.to_string(index=False))
    return {"compared": True, "all_match": bool((df["status"] == "MATCH").all())}


def report(run_id: str, as_of: str) -> dict:
    import pandas as pd
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    con = wh.connect()
    text = render((cfg.SQL_DIR / "48_business_questions.sql").read_text(),
                  {"run_id": safe_run_id(run_id)})
    titles = [l[3:] for l in text.splitlines() if l.startswith("-- Q")]
    for title, stmt in zip(titles, statements(text)):
        print(f"\n[Report] {title}")
        print(con.execute(stmt).df().to_string(index=False))
    total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM elt_fact_orders").fetchone()[0]
    con.close()
    print(f"\n[Report] total net revenue in elt_fact_orders: {total}")
    return {"total_net_revenue": float(total or 0.0)}


# ================================================================== AUDIT
def audit(run_id: str, start_time, raw_results: list, status: str) -> dict:
    rid = safe_run_id(run_id)
    extracted = sum(int(e.get("rows", 0)) for e in (raw_results or []) if isinstance(e, dict))
    quarantined = 0
    con = wh.connect()
    for t in ("stg_orders", "stg_orders_increment", "stg_payments", "stg_customers", "stg_products", "stg_stores"):
        try:
            if t == "stg_orders_increment":
                continue          # these rows are already inside stg_orders after the MERGE
            quarantined += con.execute(f"SELECT COUNT(*) FROM {t} WHERE dq_status = 'QUARANTINED' "
                                       f"AND _run_id = ?", [rid]).fetchone()[0]
        except Exception:
            pass
    row = {"run_id": rid, "dag_id": DAG_ID, "start_time": wh.to_ist(start_time),
           "end_time": wh.now_ist(), "rows_extracted": extracted,
           "rows_loaded": extracted,                 # ELT loads every row into raw
           "rows_quarantined": int(quarantined), "status": status}
    wh.write_pipeline_run(con, row)
    con.close()
    print(f"[Audit] {row}")
    return {k: str(v) for k, v in row.items()}
