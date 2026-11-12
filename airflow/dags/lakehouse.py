"""Two DAGs over the lake. The streaming job keeps bronze and silver current
on its own; these build everything that needs a full pass over history."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

STEP = "python -m lakehouse.gold {}"

common = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    "lakehouse_gold",
    description="quality checks, then SCD2 history, gold marts and the dashboard export",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=common,
    tags=["lakehouse"],
) as gold:
    # A failed check fails this task and stops the run here, so the gold
    # tables and the dashboard keep the last numbers that passed.
    quality = BashOperator(task_id="quality", bash_command=STEP.format("quality"), retries=0)
    history = BashOperator(task_id="scd2_history", bash_command=STEP.format("history"))
    marts = BashOperator(task_id="gold_marts", bash_command=STEP.format("marts"))
    export = BashOperator(task_id="export_dashboard", bash_command=STEP.format("export"))
    quality >> history >> marts >> export

with DAG(
    "lakehouse_maintenance",
    description="compact small files and expire old snapshots",
    schedule="0 21 * * *",  # 02:30 IST, when the gold DAG is the only other thing running
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=common,
    tags=["lakehouse"],
) as maintenance:
    BashOperator(task_id="compact_and_expire", bash_command=STEP.format("maintain"))
