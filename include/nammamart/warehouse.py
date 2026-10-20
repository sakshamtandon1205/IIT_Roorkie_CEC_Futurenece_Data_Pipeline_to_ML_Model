"""
DuckDB helpers used by both pipelines: connection, run-audit and quality tables.

Times are stored in IST (Asia/Kolkata) so they read naturally in screenshots.
"""
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from nammamart.config import WAREHOUSE_DB

IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    return datetime.now(IST).replace(tzinfo=None, microsecond=0)


def to_ist(dt) -> datetime:
    """Convert a datetime to a naive IST datetime. A naive input is assumed to be IST already."""
    if dt is None:
        return now_ist()
    if dt.tzinfo is None:
        return dt.replace(microsecond=0)
    return dt.astimezone(IST).replace(tzinfo=None, microsecond=0)


def connect(read_only: bool = False, tries: int = 20, wait: float = 3.0):
    """
    Open the warehouse. DuckDB allows one writer process at a time, and both DAGs run at
    9:00 AM, so a task that finds the file locked waits and tries again.
    """
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for _ in range(tries):
        try:
            return duckdb.connect(str(WAREHOUSE_DB), read_only=read_only)
        except duckdb.IOException as err:   # file is locked by another process
            last = err
            time.sleep(wait)
    raise last


AUDIT_DDL = [
    """CREATE TABLE IF NOT EXISTS dq_results (
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
        severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
        checked_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
        rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)""",
]


def ensure_audit_tables(con):
    for ddl in AUDIT_DDL:
        con.execute(ddl)


def write_dq_results(con, run_id: str, dag_id: str, results: list):
    """One row per check per run. Re-writing the same run replaces its rows (safe on retry)."""
    ensure_audit_tables(con)
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    if not results:
        return
    stamp = now_ist()
    df = pd.DataFrame([{
        "run_id": run_id, "dag_id": dag_id, "check_name": r["check_name"],
        "dimension": r["dimension"], "severity": r["severity"],
        "rows_checked": r["rows_checked"], "rows_failed": r["rows_failed"],
        "status": r["status"], "checked_at": stamp} for r in results])
    con.register("dq_df", df)
    con.execute("INSERT INTO dq_results SELECT * FROM dq_df")
    con.unregister("dq_df")


def write_pipeline_run(con, row: dict):
    ensure_audit_tables(con)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [row["run_id"], row["dag_id"]])
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                [row["run_id"], row["dag_id"], row["start_time"], row["end_time"],
                 row["rows_extracted"], row["rows_loaded"], row["rows_quarantined"],
                 row["status"]])


def last_check_rows(con, dag_id: str, check_name: str, before_run_id: str):
    """rows_checked from the most recent earlier run of a check (used by the volume check)."""
    try:
        ensure_audit_tables(con)
        row = con.execute(
            "SELECT rows_checked FROM dq_results WHERE dag_id = ? AND check_name = ? "
            "AND run_id <> ? ORDER BY checked_at DESC LIMIT 1",
            [dag_id, check_name, before_run_id]).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None
