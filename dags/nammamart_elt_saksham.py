"""
NammaMart ELT pipeline (Saksham Tandon)

load raw (6 files in parallel, untouched) -> stage with SQL -> MERGE increment -> marts with SQL
    -> compare with ETL -> report -> audit

Schedule: 9:00 AM IST, Monday to Saturday  (cron `0 9 * * 1-6`, timezone Asia/Kolkata).
Every transformation is a .sql file in include/sql/. Python only runs the files.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
from nammamart import config as cfg          # noqa: E402
from nammamart import elt                    # noqa: E402
from nammamart.etl import PermanentSchemaError  # noqa: E402
from nammamart.alerts import notify_failure, notify_quality  # noqa: E402

IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "saksham",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


def _p():
    ctx = get_current_context()
    return ctx, ctx["run_id"], ctx["params"]["as_of"], bool(ctx["params"]["load_increment"])


@dag(
    dag_id="nammamart_elt_saksham",
    description="NammaMart ELT: load raw as received, clean and aggregate with SQL inside DuckDB",
    # explicit timezone: a bare cron string would be read in UTC (09:00 UTC = 14:30 IST)
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),
    start_date=pendulum.datetime(2026, 9, 1, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", "saksham"],
    params={
        "load_increment": Param(False, type="boolean",
                                description="Also load orders_2026-10-01_increment.csv (appended to raw, MERGEd into staging)"),
        "as_of": Param(cfg.DEFAULT_AS_OF, type="string",
                       description="Extract date. Rows dated after it are treated as future-dated."),
    },
)
def nammamart_elt_saksham():

    @task
    def load_raw(key: str):
        ctx, run_id, as_of, inc = _p()
        try:
            return elt.load_raw(key, run_id, enabled=(key != "increment" or inc))
        except PermanentSchemaError as err:       # retrying cannot fix a missing column
            raise AirflowFailException(str(err))

    @task
    def stage(entity: str, raws: list):
        ctx, run_id, as_of, inc = _p()
        out = elt.stage(entity, run_id, as_of)
        bad = sum(v["quarantined"] for v in out.values())
        if bad:
            notify_quality(f"NammaMart ELT run {run_id}: {bad} {entity} rows failed a Critical check in staging")
        return out

    @task
    def merge_increment(orders_stage: dict):
        ctx, run_id, as_of, inc = _p()
        return elt.merge_increment(run_id, as_of)

    @task
    def dq_results(stages: list, merged: dict):
        ctx, run_id, as_of, inc = _p()
        return elt.dq_results(run_id, as_of)

    @task
    def build_fact_view(dq: dict):
        ctx, run_id, as_of, inc = _p()
        elt.build_fact_view(run_id, as_of)

    @task
    def build_mart(name: str, ready=None):
        ctx, run_id, as_of, inc = _p()
        return elt.build_mart(name, run_id, as_of)

    @task
    def compare_with_etl(marts: list):
        ctx, run_id, as_of, inc = _p()
        return elt.check_totals(run_id, as_of)

    @task
    def report(cmp: dict):
        ctx, run_id, as_of, inc = _p()
        return elt.report(run_id, as_of)

    @task(trigger_rule="all_done")
    def audit(raws: list, rep: dict):
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        # count tasks of this run that failed (Airflow 3 task API); audit runs even when others failed
        failed = ctx["ti"].get_ti_count(dag_id=dag_run.dag_id, run_ids=[dag_run.run_id],
                                        states=["failed", "upstream_failed"])
        status = "failed" if failed else "success"
        return elt.audit(ctx["run_id"], dag_run.start_date, raws, status)

    raws = [load_raw.override(task_id=f"load_raw_{k}")(k) for k in cfg.FILES]
    stages = [stage.override(task_id=f"stage_{e}")(e, raws) for e in elt.STAGES]
    orders_stage = stages[list(elt.STAGES).index("orders")]
    merged = merge_increment(orders_stage)
    dq = dq_results(stages, merged)
    view = build_fact_view(dq)
    marts = [build_mart.override(task_id=f"mart_{m}")(m, view) for m in elt.MARTS]
    cmp = compare_with_etl(marts)
    rep = report(cmp)
    audit(raws, rep)


nammamart_elt_saksham()
