"""
Prints one line per pipeline run, with its quality results, so that the observability experiments
can be compared side by side.

    python include/tools/history.py

Read-only. Personal data is never printed.
"""
from pathlib import Path

import duckdb
import pandas as pd

DB = Path(__file__).resolve().parents[1] / "warehouse_saksham.duckdb"
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 300)

con = duckdb.connect(str(DB), read_only=True)
runs = con.execute("""
    SELECT r.dag_id, r.run_id, r.start_time, r.end_time,
           ROUND(date_diff('millisecond', r.start_time, r.end_time) / 1000.0, 1) AS seconds,
           r.rows_extracted, r.rows_loaded, r.rows_quarantined, r.status,
           COUNT(d.check_name) AS checks,
           COUNT(*) FILTER (WHERE d.status = 'FAIL') AS fail,
           COUNT(*) FILTER (WHERE d.status = 'WARN') AS warn,
           MAX(d.rows_checked) FILTER (WHERE d.check_name = 'orders.order_id_unique') AS orders_seen,
           MAX(d.rows_failed) FILTER (WHERE d.check_name = 'orders.order_amount_matches_formula') AS formula_fail,
           MAX(d.rows_failed) FILTER (WHERE d.check_name = 'orders.unit_price_matches_catalogue_mrp') AS price_warn,
           MAX(d.status) FILTER (WHERE d.check_name = 'orders.volume_vs_last_run') AS volume,
           MAX(d.status) FILTER (WHERE d.check_name = 'orders.latest_order_within_1_day') AS fresh
    FROM pipeline_runs r
    LEFT JOIN dq_results d ON d.run_id = r.run_id AND d.dag_id = r.dag_id
    GROUP BY ALL ORDER BY r.start_time
""").df()
runs["run_id"] = runs["run_id"].str.replace("manual__", "m_").str.replace("scheduled__", "s_")
print(runs.to_string(index=False))
