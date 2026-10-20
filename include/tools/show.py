"""
Prints the audit and result tables so they can be screenshotted.

    python include/tools/show.py runs         pipeline_runs (run audit)
    python include/tools/show.py dq           dq_results scorecard for the latest run of each DAG
    python include/tools/show.py recon        payment reconciliation (ETL and ELT)
    python include/tools/show.py quarantine   newest quarantine file (personal data is masked)
    python include/tools/show.py mart         the six business questions from the ELT marts

Read-only. Needs `pip install duckdb pandas` on the machine that runs it.
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd

INCLUDE = Path(__file__).resolve().parents[1]
DB = INCLUDE / "warehouse_saksham.duckdb"
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", 60)


def q(con, sql, *a):
    return con.execute(sql, list(a)).df()


def main(what):
    if what == "quarantine":
        files = sorted((INCLUDE / "quarantine" / "saksham").glob("orders_failed_*.csv"),
                       key=lambda p: p.stat().st_mtime)
        if not files:
            sys.exit("no quarantine files yet, run the ETL DAG first")
        f = files[-1]
        print(f"{f.relative_to(INCLUDE.parent)}  ({sum(1 for _ in open(f)) - 1} rows)\n")
        print(pd.read_csv(f, dtype=str, keep_default_na=False).head(12).to_string(index=False))
        return
    con = duckdb.connect(str(DB), read_only=True)
    if what == "runs":
        print(q(con, "SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, "
                     "rows_quarantined, status FROM pipeline_runs ORDER BY start_time").to_string(index=False))
    elif what == "dq":
        for dag in ("nammamart_etl_saksham", "nammamart_elt_saksham"):
            run = con.execute("SELECT run_id FROM dq_results WHERE dag_id = ? ORDER BY checked_at DESC LIMIT 1",
                              [dag]).fetchone()
            if not run:
                continue
            print(f"\n=== {dag}   run {run[0]} ===")
            print(q(con, "SELECT dimension, COUNT(*) AS checks, COUNT(*) FILTER (WHERE status='PASS') AS pass, "
                         "COUNT(*) FILTER (WHERE status='WARN') AS warn, COUNT(*) FILTER (WHERE status='FAIL') AS fail "
                         "FROM dq_results WHERE dag_id = ? AND run_id = ? GROUP BY 1 ORDER BY 1",
                    dag, run[0]).to_string(index=False))
            print("\nchecks that found problems:")
            print(q(con, "SELECT check_name, dimension, severity, rows_checked, rows_failed, status "
                         "FROM dq_results WHERE dag_id = ? AND run_id = ? AND rows_failed > 0 "
                         "ORDER BY dimension, check_name LIMIT 45", dag, run[0]).to_string(index=False))
    elif what == "recon":
        for title, table in (("ETL  etl_payment_reconciliation", "etl_payment_reconciliation"),
                             ("ELT  mart_payment_reconciliation", "mart_payment_reconciliation")):
            print(f"\n=== {title} ===")
            print(q(con, f"SELECT mismatch_type, COUNT(*) AS orders, ROUND(SUM(ABS(difference)), 2) AS amount "
                         f"FROM {table} GROUP BY 1 ORDER BY 2 DESC").to_string(index=False))
        print("\nsample rows (ETL):")
        print(q(con, "SELECT order_id, payment_id, mismatch_type, order_amount, amount_paid, difference, reason "
                     "FROM etl_payment_reconciliation ORDER BY mismatch_type, order_id LIMIT 12").to_string(index=False))
    elif what == "mart":
        for t, sql in (("mart_daily_store_kpis (first rows)", "SELECT order_date, zone, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis ORDER BY 1,2,3 LIMIT 8"),
                       ("mart_category_margin", "SELECT * FROM mart_category_margin ORDER BY revenue DESC"),
                       ("mart_delivery_sla", "SELECT * FROM mart_delivery_sla ORDER BY store_id"),
                       ("mart_revenue_by_payment_mode", "SELECT * FROM mart_revenue_by_payment_mode ORDER BY revenue DESC"),
                       ("mart_top_customers", "SELECT * FROM mart_top_customers")):
            print(f"\n=== {t} ===")
            print(q(con, sql).to_string(index=False))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
