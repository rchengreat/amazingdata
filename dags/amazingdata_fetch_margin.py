# -*- coding: utf-8 -*-
"""
amazingdata_fetch_margin.py

DAG: amazingdata_fetch_margin
Schedule: 工作日 16:15

Tasks:
  fetch_margin — margin_summary_history.parquet（增量）
              — margin_detail_history.parquet（增量）
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
    dag_id="amazingdata_fetch_margin",
    default_args=default_args,
    schedule="15 16 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "margin", "daily"],
    description="工作日 16:15 拉取 margin_summary_history 和 margin_detail_history",
) as dag:

    fetch_margin = BashOperator(
        task_id="fetch_margin",
        bash_command=(
            "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
            "python3 /opt/airflow/scripts_ad/fetch_margin.py"
        ),
    )
