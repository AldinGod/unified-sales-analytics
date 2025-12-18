from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ⚙️ Root tvog projekta
PROJECT_ROOT = "/Users/aldingodinjak/unified-sales-analytics"
VENV_ACTIVATE = f"source {PROJECT_ROOT}/.venv/bin/activate"

default_args = {
    "owner": "aldin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_batch_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold → Postgres batch pipeline za unified-sales-analytics",
    schedule_interval="@daily",        # pokreće se jednom dnevno
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "spark", "delta", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{VENV_ACTIVATE} && "
            "unset SPARK_HOME && "
            "python spark-jobs/batch/bronze_to_silver.py"
        ),
    )


    silver_to_gold = BashOperator(
     task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{VENV_ACTIVATE} && "
            "unset SPARK_HOME && "
            "python spark-jobs/batch/silver_to_gold.py"
        ),
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{VENV_ACTIVATE} && "
            "python spark-jobs/batch/gold_to_postgres.py"
        ),
    )

    notify_done = BashOperator(
        task_id="notify_done",
        bash_command='echo "Sales batch pipeline finished for {{ ds }}"',
    )

    end = EmptyOperator(task_id="end")

    # Orkestracija:
    # start → bronze_to_silver → silver_to_gold → gold_to_postgres → notify_done → end
    start >> bronze_to_silver >> silver_to_gold >> gold_to_postgres >> notify_done >> end
