"""
NammaMart ETL pipeline (Saksham Tandon)

extract (6 files in parallel) -> validate -> quarantine -> transform -> load -> reconcile -> report -> audit

Schedule: 9:00 AM IST, Monday to Saturday  (cron `0 9 * * 1-6`, timezone Asia/Kolkata).
All logic is in include/nammamart/etl.py so it can be tested without Airflow.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
from nammamart import config as cfg          # noqa: E402
from nammamart import etl                    # noqa: E402
from nammamart.alerts import notify_failure, notify_quality  # noqa: E402

IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "saksham",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="nammamart_etl_saksham",
    description="NammaMart ETL: validate and clean in Python, then load a star schema into DuckDB",
    # explicit timezone: a bare cron string would be read in UTC (09:00 UTC = 14:30 IST)
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),
    start_date=pendulum.datetime(2026, 9, 1, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", "saksham"],
    params={
        "load_increment": Param(False, type="boolean",
                                description="Also load orders_2026-10-01_increment.csv (new orders + status corrections)"),
        "as_of": Param(cfg.DEFAULT_AS_OF, type="string",
                       description="Extract date. Rows dated after it are treated as future-dated."),
    },
)
def nammamart_etl_saksham():

    @task
    def extract(key: str):
        ctx = get_current_context()
        enabled = key != "increment" or bool(ctx["params"]["load_increment"])
        return etl.extract(key, ctx["run_id"], enabled)

    @task
    def validate(extracts: list):
        ctx = get_current_context()
        return etl.validate(extracts, ctx["run_id"], ctx["params"]["as_of"],
                            bool(ctx["params"]["load_increment"]))

    @task
    def quarantine(val: dict):
        ctx = get_current_context()
        out = etl.quarantine(val, ctx["run_id"])
        if out["quarantined"]:
            notify_quality(f"NammaMart ETL run {ctx['run_id']}: {out['quarantined']} rows quarantined "
                           f"({out['per_file']})")
        return out

    @task
    def transform(val: dict, q: dict):
        ctx = get_current_context()
        return etl.transform(val, ctx["run_id"], bool(ctx["params"]["load_increment"]))

    @task
    def load(val: dict, t: dict):
        ctx = get_current_context()
        return etl.load(val["counts"], ctx["run_id"], bool(ctx["params"]["load_increment"]))

    @task
    def reconcile(l: dict):
        ctx = get_current_context()
        return etl.reconcile(ctx["run_id"], bool(ctx["params"]["load_increment"]))

    @task
    def report(r: dict):
        return etl.report(get_current_context()["run_id"])

    @task(trigger_rule="all_done")
    def audit(extracts: list, q: dict, l: dict):
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        # count tasks of this run that failed (Airflow 3 task API); audit runs even when others failed
        failed = ctx["ti"].get_ti_count(dag_id=dag_run.dag_id, run_ids=[dag_run.run_id],
                                        states=["failed", "upstream_failed"])
        status = "failed" if failed else "success"
        return etl.audit(ctx["run_id"], dag_run.start_date, extracts, q, l, status)

    extracts = [extract.override(task_id=f"extract_{k}")(k) for k in cfg.FILES]
    val = validate(extracts)
    q = quarantine(val)
    t = transform(val, q)
    l = load(val, t)
    r = reconcile(l)
    rep = report(r)
    a = audit(extracts, q, l)
    rep >> a


nammamart_etl_saksham()
