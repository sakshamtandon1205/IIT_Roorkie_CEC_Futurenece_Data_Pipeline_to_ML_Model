"""Failure alert (bonus). The webhook URL is read from the Airflow Variable SLACK_WEBHOOK_URL,
never written in code. If the Variable is not set, the message is printed to the task log."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # make include/ importable
from slack_notifier import send_slack_alert                      # noqa: E402


def _webhook():
    try:
        from airflow.sdk import Variable
        return Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:
        return None


def notify_failure(context):
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    msg = (f":red_circle: NammaMart pipeline failed\nDAG: {dag_id}\nTask: {task_id}\n"
           f"Run: {run_id}\nException: {context.get('exception')}")
    send_slack_alert(msg, webhook_url=_webhook())


def notify_quality(message: str):
    send_slack_alert(message, webhook_url=_webhook())
