# -*- coding: utf-8 -*-
"""
amazingdata_fetch_equity.py

DAG: amazingdata_fetch_equity
Schedule: 工作日 04:30

Tasks:
  fetch_equity — equity_structure_history.parquet（增量）
               — equity_dividend_history.parquet（增量）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="amazingdata_fetch_equity",
    default_args=default_args,
    schedule="30 4 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "equity", "daily"],
    description="工作日 04:30 拉取 equity_structure_history 和 equity_dividend_history",
) as dag:

    fetch_equity = BashOperator(
        task_id="fetch_equity",
        bash_command=(
            "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
            "python3 /opt/airflow/scripts_ad/fetch_equity.py"
        ),
    )
